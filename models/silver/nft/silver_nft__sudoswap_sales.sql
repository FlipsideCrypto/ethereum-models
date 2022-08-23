{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH swap_details AS (

    SELECT
        *,
        SUBSTR(
            input,
            0,
            10
        ) :: STRING AS function_call,
        regexp_substr_all(SUBSTR(input, 11, len(input)), '.{64}') AS segmented_input,
        regexp_substr_all(SUBSTR(output, 3, len(output)), '.{64}') AS segmented_output,
        PUBLIC.udf_hex_to_int(
            segmented_input [0] :: STRING
        ) :: INTEGER / pow(
            10,
            18
        ) AS amount_per,
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
        ) AS row_no,
        CASE
            WHEN function_call = '0x7ca542ac' THEN (
                total_amount / nft_count
            ) - amount_per
            WHEN function_call = '0x097cc63d' THEN amount_per - (
                total_amount / nft_count
            )
        END AS sudo_fee
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
sudo_interactions AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_number,
        to_address AS origin_to_address,
        from_address AS origin_from_address,
        tx_fee,
        origin_function_signature
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        block_number > 14000000
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                swap_details
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
add_position AS (
    SELECT
        tx_hash,
        from_address,
        to_address,
        amount_per,
        total_amount,
        sudo_fee,
        nft_count,
        CEIL(MIN(row_no) / 2) AS agg_id1
    FROM
        swap_details
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7),
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
                add_position
        ),
        nft_sales AS (
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
                ROW_NUMBER() over (
                    PARTITION BY tx_hash
                    ORDER BY
                        event_index ASC
                ) AS agg_id,
                _log_id,
                _inserted_timestamp
            FROM
                {{ ref('silver__nft_transfers') }}
            WHERE
                tx_hash IN (
                    SELECT
                        DISTINCT tx_hash
                    FROM
                        amounts_and_counts
                )
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
                amount_per + sudo_fee AS price,
                sudo_fee AS platform_fee,
                _log_id,
                _inserted_timestamp
            FROM
                nft_sales
                LEFT JOIN amounts_and_counts
                ON nft_sales.tx_hash = amounts_and_counts.tx_hash
                AND agg_id BETWEEN agg_id_min
                AND agg_id_max
        ),
        usd_prices AS (
            SELECT
                HOUR,
                AVG(price) AS eth_price
            FROM
                {{ ref('core__fact_hourly_token_prices') }}
            WHERE
                token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                AND HOUR :: DATE IN (
                    SELECT
                        DISTINCT block_timestamp :: DATE
                    FROM
                        sudo_interactions
                )
            GROUP BY
                HOUR
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
        'ETH' AS currency_symbol,
        'ETH' AS currency_address,
        price,
        ROUND(
            price * eth_price,
            2
        ) AS price_usd,
        platform_fee AS total_fees,
        platform_fee,
        0 AS creator_fee,
        total_fees * eth_price AS total_fees_usd,
        platform_fee * eth_price AS platform_fee_usd,
        0 AS creator_fee_usd,
        tx_fee,
        ROUND(
            tx_fee * eth_price,
            2
        ) AS tx_fee_usd,
        _log_id,
        _inserted_timestamp
    FROM
        swap_final
        LEFT JOIN sudo_interactions
        ON swap_final.tx_hash = sudo_interactions.tx_hash
        LEFT JOIN usd_prices
        ON DATE_TRUNC(
            'hour',
            block_timestamp
        ) = HOUR
    WHERE
        price IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY _log_id
    ORDER BY
        _inserted_timestamp DESC)) = 1
