{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}
-- start by finding sales in traces
WITH sudoswap_tx as (
    SELECT  
        tx_hash 
    FROM 
        {{ ref('silver__logs') }}
    WHERE 
        block_timestamp >= '2022-01-01'
        AND topics[0] = '0xf06180fdbe95e5193df4dcd1352726b1f04cb58599ce58552cc952447af2ffbb'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),


sale_data1 AS (

    SELECT
        tx_hash,
        from_address,
        to_address,
        block_timestamp,
        CASE
            WHEN from_address = '0x2b2e8cda09bba9660dca5cb6233787738ad68329' THEN 'sale'
            WHEN to_address = '0xb16c1342e617a5b6e4b631eb114483fdb289c0a4' THEN 'fee'
        END AS payment_type,
        eth_value,
        identifier,
        CASE
            WHEN from_address = '0x2b2e8cda09bba9660dca5cb6233787738ad68329' THEN 'normal'
            ELSE 'fee'
        END AS flag
    FROM
        {{ ref('silver__traces') }}
    WHERE
        block_number > 14000000
        AND (
            from_address = '0x2b2e8cda09bba9660dca5cb6233787738ad68329'
            OR to_address = '0xb16c1342e617a5b6e4b631eb114483fdb289c0a4'
        )
        AND TYPE = 'CALL'
        AND identifier <> 'CALL_ORIGIN'
        AND eth_value > 0
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
non_buys AS (
    SELECT
        A.tx_hash,
        A.from_address,
        A.to_address,
        A.block_timestamp,
        CASE
            WHEN A.to_address = '0xb16c1342e617a5b6e4b631eb114483fdb289c0a4' THEN 'fee'
            ELSE 'sale'
        END AS payment_type,
        A.eth_value,
        A.identifier,
        CASE
            WHEN A.to_address = b.from_address
            AND A.from_address <> '0x2b2e8cda09bba9660dca5cb6233787738ad68329' THEN 'pool_fee'
            ELSE 'non'
        END AS flag
    FROM
        {{ ref('silver__traces') }} A
        INNER JOIN sale_data1 b
        ON A.tx_hash = b.tx_hash
        AND (
            A.from_address = b.from_address
            OR A.to_address = b.from_address
        )
        AND b.payment_type = 'fee'
    WHERE
        block_number > 14000000
        AND A.type = 'CALL'
        AND A.identifier <> 'CALL_ORIGIN'
        AND A.eth_value > 0
        AND A.tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
count_details AS (
    SELECT
        tx_hash,
        from_address,
        to_address,
        block_timestamp,
        identifier,
        SUBSTR(
            input,
            0,
            10
        ) :: STRING AS function_call,
        regexp_substr_all(SUBSTR(input, 11, len(input)), '.{64}') AS segmented_input,
        regexp_substr_all(SUBSTR(output, 3, len(output)), '.{64}') AS segmented_output,
        PUBLIC.udf_hex_to_int(
            segmented_input [2] :: STRING
        ) :: INTEGER AS nft_count,
        PUBLIC.udf_hex_to_int(
            segmented_output [3] :: STRING
        ) :: INTEGER / pow(
            10,
            18
        ) AS total_amount,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                identifier ASC
        ) AS row_no
    FROM
        {{ ref('silver__traces') }}
    WHERE
        block_number > 14000000
        AND TYPE = 'STATICCALL'
        AND SUBSTR(
            input,
            0,
            10
        ) :: STRING IN (
            '0x7ca542ac',
            '0x097cc63d'
        )
        AND tx_hash in (
            SELECT 
                tx_hash 
            FROM sudoswap_tx
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
union_records1 AS (
    SELECT
        tx_hash,
        from_address,
        to_address,
        block_timestamp,
        payment_type,
        eth_value,
        MAX(identifier) AS identifier,
        MAX(flag) AS flag
    FROM
        (
            SELECT
                tx_hash,
                from_address,
                to_address,
                block_timestamp,
                payment_type,
                eth_value,
                identifier,
                flag
            FROM
                sale_data1
            UNION ALL
            SELECT
                tx_hash,
                from_address,
                to_address,
                block_timestamp,
                payment_type,
                eth_value,
                identifier,
                flag
            FROM
                non_buys
            UNION ALL
            SELECT
                tx_hash,
                from_address,
                to_address,
                block_timestamp,
                'sale' AS payment_type,
                total_amount AS eth_value,
                identifier,
                'no_fee' AS flag
            FROM
                count_details
            WHERE
                tx_hash NOT IN (
                    SELECT
                        DISTINCT tx_hash
                    FROM
                        non_buys
                )
                AND tx_hash NOT IN (
                    SELECT
                        DISTINCT tx_hash
                    FROM
                        sale_data1
                )
        )
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6
),
origin_details AS (
    SELECT
        tx_hash,
        from_address,
        to_address,
        block_timestamp,
        'sale' AS payment_type,
        eth_value,
        'origin' AS identifier,
        'private' AS flag
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        block_number > 14000000
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                union_records1
        )
        AND tx_hash NOT IN (
            SELECT
                DISTINCT tx_hash
            FROM
                union_records1
            WHERE
                payment_type = 'sale'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
union_records AS (
    SELECT
        tx_hash,
        from_address,
        to_address,
        block_timestamp,
        payment_type,
        eth_value,
        identifier,
        flag
    FROM
        union_records1
    UNION ALL
    SELECT
        tx_hash,
        from_address,
        to_address,
        block_timestamp,
        payment_type,
        eth_value,
        identifier,
        flag
    FROM
        origin_details
),
sale_data AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            payment_type
            ORDER BY
                identifier ASC
        ) AS row_no
    FROM
        union_records
),
dedup_counts AS (
    SELECT
        tx_hash,
        from_address,
        to_address,
        total_amount,
        nft_count,
        CEIL(MIN(row_no) / 2) AS agg_id1
    FROM
        count_details
    GROUP BY
        1,
        2,
        3,
        4,
        5),
        sudo_sales AS (
            SELECT
                A.tx_hash,
                A.eth_value AS sale_amt,
                nft_count,
                b.eth_value AS fee_amt,
                A.row_no AS row_no,
                CASE
                    WHEN A.flag IN (
                        'normal',
                        'pool_fee'
                    ) THEN fee_amt / nft_count
                    ELSE fee_amt / nft_count
                END AS fee_per,
                CASE
                    WHEN A.flag IN (
                        'normal',
                        'pool_fee',
                        'non',
                        'private'
                    ) THEN sale_amt / nft_count
                    ELSE (
                        sale_amt / nft_count
                    ) - COALESCE(
                        fee_per,
                        0
                    )
                END AS amount_per,
                C.agg_id1
            FROM
                sale_data A
                LEFT JOIN sale_data b
                ON A.tx_hash :: STRING = b.tx_hash :: STRING
                AND A.row_no = b.row_no
                AND b.payment_type = 'fee'
                LEFT JOIN dedup_counts C
                ON A.tx_hash :: STRING = C.tx_hash :: STRING
                AND A.row_no = C.agg_id1
            WHERE
                A.payment_type = 'sale'
        ),
        amounts_and_counts AS (
            SELECT
                *,
                COALESCE(LAG(nft_count) over (PARTITION BY tx_hash
            ORDER BY
                agg_id1 ASC), 0) AS prev_nft_count,
                COALESCE(LAG(agg_id1) over (PARTITION BY tx_hash
            ORDER BY
                agg_id1 ASC), 0) AS prev_agg_id,
                SUM(nft_count) over (
                    PARTITION BY tx_hash
                    ORDER BY
                        agg_id1 ASC rows BETWEEN unbounded preceding
                        AND CURRENT ROW
                ) -1 AS cum_nfts,
                cum_nfts + 1 AS agg_id_max,
                agg_id_max - (
                    nft_count -1
                ) AS agg_id_min
            FROM
                sudo_sales
        ),
        sudo_interactions AS (
            SELECT
                tx_hash,
                block_timestamp,
                block_number,
                to_address AS origin_to_address,
                from_address AS origin_from_address,
                tx_fee,
                origin_function_signature,
                input_data
            FROM
                {{ ref('silver__transactions') }}
            WHERE
                block_number > 14000000
                AND tx_hash IN (
                    SELECT
                        DISTINCT tx_hash
                    FROM
                        sale_data
                )
                AND status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
sudo_events AS (
    SELECT
        sudo_event_hash,
        MAX(sudo_event_index_join) AS max_sudo_event_index_join,
        MIN(sudo_event_index_join) AS min_sudo_event_index_join
    FROM
        (
            SELECT
                tx_hash AS sudo_event_hash,
                event_index AS sudo_event_index,
                event_index + 1 AS sudo_event_index_join
            FROM
                {{ ref('silver__logs') }}
            WHERE
                topics [0] :: STRING = '0xf06180fdbe95e5193df4dcd1352726b1f04cb58599ce58552cc952447af2ffbb'
                AND tx_hash IN (
                    SELECT
                        DISTINCT tx_hash
                    FROM
                        sale_data
                )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
)
GROUP BY
    1
),
nft_sales AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS agg_id
    FROM
        (
            SELECT
                tx_hash,
                event_index,
                contract_address,
                tokenid,
                from_address,
                to_address,
                erc1155_value,
                project_name,
                token_metadata,
                _log_id,
                _inserted_timestamp,CASE
                    WHEN flag IN (
                        'no_fee',
                        'non'
                    ) THEN 'y'
                    WHEN event_index >= min_sudo_event_index_join THEN 'y'
                END AS record_type
            FROM
                {{ ref('silver__nft_transfers') }}
                LEFT JOIN sudo_events
                ON tx_hash = sudo_event_hash
                LEFT JOIN (
                    SELECT
                        DISTINCT tx_hash AS sale_tx_hash,
                        flag
                    FROM
                        sale_data
                    WHERE
                        flag IN (
                            'non',
                            'no_fee'
                        )
                )
                ON sale_tx_hash = tx_hash
            WHERE
                tx_hash IN (
                    SELECT
                        DISTINCT tx_hash
                    FROM
                        sale_data
                )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
)
WHERE
    record_type = 'y'
),
swap_final AS (
    SELECT
        nft_sales.tx_hash,
        contract_address AS nft_address,
        event_index,
        tokenid,
        erc1155_value,
        project_name,
        token_metadata,
        nft_sales.from_address AS seller_address,
        nft_sales.to_address AS buyer_address,
        amount_per AS price,
        fee_per AS platform_fee,
        _log_id,
        _inserted_timestamp
    FROM
        nft_sales
        LEFT JOIN amounts_and_counts
        ON nft_sales.tx_hash = amounts_and_counts.tx_hash
        AND agg_id BETWEEN agg_id_min
        AND agg_id_max

    WHERE 
        nft_sales.tx_hash in (
            select 
            tx_hash 
            from sudoswap_tx
        )
),
token_transfers AS (
    SELECT
        DISTINCT tx_hash,
        contract_address
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        block_number > 15000000
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                swap_final
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
decimals AS (
    SELECT
        address,
        symbol,
        decimals
    FROM
        {{ ref('silver__contracts') }}
    WHERE
        address IN (
            SELECT
                DISTINCT contract_address
            FROM
                token_transfers
        )
),
usd_prices AS (
    SELECT
        HOUR,
        token_address,
        AVG(price) AS token_price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        (
            token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            OR (
                token_address IN (
                    SELECT
                        DISTINCT address
                    FROM
                        decimals
                )
            )
        )
    GROUP BY
        HOUR,
        token_address
),
eth_prices AS (
    SELECT
        HOUR,
        token_price AS eth_price
    FROM
        usd_prices
    WHERE
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
)
SELECT
    block_number,
    block_timestamp,
    swap_final.tx_hash,
    origin_to_address,
    origin_from_address,
    origin_function_signature,
    'sale' AS event_type,
    '0x2b2e8cda09bba9660dca5cb6233787738ad68329' AS platform_address,
    'sudoswap' AS platform_name,
    'pair router' AS platform_exchange_version,
    buyer_address,
    seller_address,
    nft_address,
    project_name,
    erc1155_value,
    tokenId,
    token_metadata,
    COALESCE(
        decimals.symbol,
        'ETH'
    ) AS currency_symbol,
    COALESCE(
        token_transfers.contract_address,
        'ETH'
    ) AS currency_address,
    price,
    ROUND(
        price * token_price,
        2
    ) AS price_usd,
    COALESCE(
        platform_fee,
        0
    ) AS total_fees,
    COALESCE(
        platform_fee,
        0
    ) AS platform_fee,
    0 AS creator_fee,
    total_fees * token_price AS total_fees_usd,
    COALESCE(
        platform_fee,
        0
    ) * token_price AS platform_fee_usd,
    0 AS creator_fee_usd,
    tx_fee,
    ROUND(
        tx_fee * eth_price,
        2
    ) AS tx_fee_usd,
    _log_id,
    _inserted_timestamp,
    sudo_interactions.input_data 
FROM
    swap_final
    LEFT JOIN sudo_interactions
    ON swap_final.tx_hash = sudo_interactions.tx_hash
    LEFT JOIN token_transfers
    ON swap_final.tx_hash = token_transfers.tx_hash
    LEFT JOIN decimals
    ON token_transfers.contract_address = decimals.address
    LEFT JOIN usd_prices
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = usd_prices.hour
    AND (
        CASE
            WHEN token_transfers.contract_address IS NULL THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE token_transfers.contract_address
        END
    ) = usd_prices.token_address
    LEFT JOIN eth_prices
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = eth_prices.hour
WHERE
    price IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
