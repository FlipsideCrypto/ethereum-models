{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH seaport_interactions AS (

    SELECT
        tx_hash,
        block_timestamp,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        ARRAY_SIZE(segmented_data) AS size_array,
        event_index
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_number > 14000000
        AND contract_address = '0x00000000006c3852cbef3e08e8df289169ede581'
        AND topics [0] :: STRING = '0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31'

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
tokens AS (
    SELECT
        DISTINCT address
    FROM
        {{ ref('core__dim_contracts') }}
    WHERE
        decimals IS NOT NULL
),
max_array_values AS (
    SELECT
        tx_hash,
        event_index,
        block_timestamp,
        segmented_data,
        address AS token_address
    FROM
        seaport_interactions
        LEFT JOIN tokens b
        ON CONCAT('0x', SUBSTR(segmented_data [6] :: STRING, 25, 40)) = b.address
    WHERE
        size_array >= 25
),
ditinct_nft_transfers AS (
    SELECT
        DISTINCT tx_hash,
        contract_address
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                seaport_interactions
        )
),
flat_interactions AS (
    SELECT
        tx_hash,
        event_index,
        block_timestamp,
        token_address,
        CONCAT('0x', SUBSTR(VALUE :: STRING, 25, 40)) AS token_contract_address,
        INDEX,
        VALUE :: STRING AS json_out,
        LAG(
            json_out,
            1
        ) over (
            PARTITION BY tx_hash
            ORDER BY
                INDEX ASC
        ) AS lag1,
        LAG(
            json_out,
            2
        ) over (
            PARTITION BY tx_hash
            ORDER BY
                INDEX ASC
        ) AS lag2,
        COALESCE(
            json_out :: STRING = lag2 :: STRING,
            FALSE
        ) AS max_index_flag,
        COALESCE(
            json_out :: STRING = lag1 :: STRING,
            FALSE
        ) AS max_index_flag1,
        CASE
            WHEN token_address = token_contract_address THEN 3
            ELSE 4
        END AS OPERATOR,
        LAG(
            OPERATOR,
            -1
        ) over (
            PARTITION BY tx_hash
            ORDER BY
                INDEX ASC
        ) AS lag_operator
    FROM
        max_array_values,
        LATERAL FLATTEN(
            input => segmented_data
        )
),
max_nft_transfer AS (
    SELECT
        tx_hash,
        event_index,
        MAX(INDEX) AS index_filter
    FROM
        flat_interactions
    WHERE
        max_index_flag = TRUE
    GROUP BY
        tx_hash,
        event_index
),
find_relevant_indicies AS (
    SELECT
        A.*
    FROM
        flat_interactions A
        INNER JOIN ditinct_nft_transfers b
        ON A.tx_hash = b.tx_hash
        AND token_contract_address = b.contract_address
        LEFT JOIN max_nft_transfer C
        ON A.tx_hash = C.tx_hash
        AND A.event_index = C.event_index
    WHERE
        A.index < index_filter
),
max_index AS (
    SELECT
        tx_hash,
        event_index,
        MIN(lag_operator) AS lag_operator,
        MIN(INDEX) AS min_index,
        MAX(INDEX) + 1 AS max_index
    FROM
        find_relevant_indicies
    GROUP BY
        tx_hash,
        event_index
),
offset_id AS (
    SELECT
        tx_hash,
        event_index,
        lag_operator,
        max_index,
        min_index,
        CASE
            WHEN lag_operator = 3 THEN 1
            ELSE 0
        END AS offset
    FROM
        max_index
),
relevant_interactions AS (
    SELECT
        A.tx_hash,
        A.event_index,
        block_timestamp,
        CASE
            WHEN offset_id.lag_operator = 3 THEN 3
            ELSE 4
        END AS mod_offset,
        CASE
            WHEN MOD(
                INDEX,
                mod_offset
            ) = 2 THEN CONCAT('0x', SUBSTR(json_out, 25, 40))
            ELSE ethereum.public.udf_hex_to_int(json_out)
        END AS output_value,
        CASE
            WHEN MOD(
                INDEX,
                mod_offset
            ) = 2 THEN 'nft_address'
            ELSE 'tokenid'
        END AS output_type,
        CEIL(ROW_NUMBER() over (PARTITION BY A.tx_hash, A.event_index
    ORDER BY
        INDEX ASC) / 2) AS agg_id,
        MOD(
            INDEX,
            mod_offset
        ),
        INDEX
    FROM
        flat_interactions A
        LEFT JOIN offset_id
        ON A.tx_hash = offset_id.tx_hash
        AND A.event_index = offset_id.event_index
    WHERE
        (
            INDEX
        ) >= min_index
        AND (
            INDEX
        ) <= (
            max_index
        )
        AND (
            MOD(
                INDEX,
                mod_offset
            ) IN (
                2,
                3
            )
            OR token_address IS NOT NULL
            AND MOD(
                INDEX,
                mod_offset
            ) = 0
        )
),
seaport_sales AS (
    SELECT
        tx_hash,
        event_index,
        block_timestamp,
        agg_id,
        MAX(
            CASE
                WHEN output_type = 'nft_address' THEN output_value
            END
        ) AS nft_address,
        MAX(
            CASE
                WHEN output_type = 'tokenid' THEN output_value
            END
        ) AS tokenid
    FROM
        relevant_interactions
    GROUP BY
        tx_hash,
        event_index,
        block_timestamp,
        agg_id
),
nft_transfers AS (
    SELECT
        tx_hash,
        contract_address AS nft_address,
        project_name,
        from_address,
        to_address,
        tokenid,
        token_metadata,
        erc1155_value,
        ingested_at,
        _log_id,
        event_index
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        block_number > 14000000
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                seaport_sales
        )
        AND to_address <> '0x0000000000000000000000000000000000000000'

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
relevant_transfers AS (
    SELECT
        A.*,
        ROW_NUMBER() over(
            PARTITION BY A.tx_hash
            ORDER BY
                A.event_index ASC
        ) AS agg_id
    FROM
        nft_transfers A
        INNER JOIN seaport_sales b
        ON A.tx_hash = b.tx_hash
        AND A.nft_address = b.nft_address
        AND A.tokenid = b.tokenid
),
eth_tx_data AS (
    SELECT
        A.tx_hash,
        A.from_address,
        A.to_address,
        A.eth_value,
        A.identifier,
        LEFT(A.identifier, LENGTH(A.identifier) -2) AS id_group,
        CASE
            WHEN A.to_address = '0x5b3256965e7c3cf26e11fcaf296dfc8807c01073' THEN 'os_fee' --fee managment contract
            WHEN A.to_address = b.from_address THEN 'to_seller'
            ELSE 'royalty'
        END AS payment_type,
        SPLIT(
            identifier,
            '_'
        ) AS split_id,
        split_id [1] :: INTEGER AS level1,
        split_id [2] :: INTEGER AS level2,
        split_id [3] :: INTEGER AS level3,
        split_id [4] :: INTEGER AS level4,
        split_id [5] :: INTEGER AS level5,
        split_id [6] :: INTEGER AS level6,
        split_id [7] :: INTEGER AS level7,
        split_id [8] :: INTEGER AS level8,
        'ETH' AS currency_symbol,
        'ETH' AS currency_address
    FROM
        {{ ref('silver__eth_transfers') }} A full
        OUTER JOIN (
            SELECT
                DISTINCT tx_hash,
                from_address
            FROM
                relevant_transfers
        ) b
        ON A.tx_hash = b.tx_hash
        AND A.to_address = b.from_address
    WHERE
        block_number > 14000000
        AND A.tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                seaport_sales
        )
        AND A.from_address = '0x00000000006c3852cbef3e08e8df289169ede581' --exchange contract

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
count_type AS (
    SELECT
        tx_hash,
        SUM(
            CASE
                WHEN payment_type = 'os_fee' THEN 1
            END
        ) AS os_fee_count,
        SUM(
            CASE
                WHEN payment_type = 'royalty' THEN 1
            END
        ) AS royalty_count,
        SUM(
            CASE
                WHEN payment_type = 'to_seller' THEN 1
            END
        ) AS seller_payment_count
    FROM
        eth_tx_data
    GROUP BY
        tx_hash
),
agg_id_type AS (
    SELECT
        tx_hash,
        CASE
            WHEN os_fee_count = royalty_count
            AND royalty_count = seller_payment_count THEN 3
            ELSE 2
        END AS agg_id_type
    FROM
        count_type
),
traces_agg_id AS (
    SELECT
        A.*,
        ROW_NUMBER() over(
            PARTITION BY A.tx_hash
            ORDER BY
                level1 ASC,
                level2 ASC,
                level3 ASC,
                level4 ASC,
                level5 ASC,
                level6 ASC,
                level7 ASC,
                level8 ASC
        ) AS agg_id,
        CEIL(
            agg_id / 3
        ) AS agg_id3,
        CEIL(
            agg_id / 2
        ) AS agg_id2,
        b.agg_id_type,
        CASE
            WHEN b.agg_id_type = 2 THEN agg_id2
            WHEN b.agg_id_type = 3 THEN agg_id3
        END AS agg_id_final
    FROM
        eth_tx_data A
        LEFT JOIN agg_id_type b
        ON A.tx_hash = b.tx_hash
    WHERE
        payment_type <> 'os_fee'
),
nft_count AS (
    SELECT
        tx_hash,
        from_address,
        COUNT(
            DISTINCT tokenid
        ) AS nft_count
    FROM
        relevant_transfers
    GROUP BY
        tx_hash,
        from_address
),
nft_count_total AS (
    SELECT
        tx_hash,
        SUM(nft_count) AS nft_count_total
    FROM
        nft_count
    GROUP BY
        tx_hash
),
os_total_eth_fees AS (
    SELECT
        tx_hash,
        SUM(eth_value) AS amount
    FROM
        eth_tx_data
    WHERE
        payment_type = 'os_fee'
    GROUP BY
        tx_hash
),
os_fees_per_nft AS (
    SELECT
        A.tx_hash,
        amount / b.nft_count_total AS os_fees_per_nft
    FROM
        os_total_eth_fees A
        LEFT JOIN nft_count_total b
        ON A.tx_hash = b.tx_hash
),
eth_sales AS (
    SELECT
        tx_hash,
        to_address,
        SUM(eth_value) AS eth_sale_amount
    FROM
        traces_agg_id
    WHERE
        payment_type = 'to_seller'
    GROUP BY
        tx_hash,
        to_address
),
eth_sales_per AS (
    SELECT
        A.tx_hash,
        A.to_address,
        A.eth_sale_amount / b.nft_count AS eth_sale_per
    FROM
        eth_sales A
        LEFT JOIN nft_count b
        ON A.tx_hash = b.tx_hash
        AND A.to_address = b.from_address
),
eth_royalties_total AS (
    SELECT
        tx_hash,
        SUM(eth_value) AS royalty_amount
    FROM
        traces_agg_id
    WHERE
        payment_type = 'royalty'
    GROUP BY
        tx_hash
),
eth_royalties AS (
    SELECT
        A.tx_hash,
        royalty_amount / b.nft_count_total AS royalty_amount
    FROM
        eth_royalties_total A
        LEFT JOIN nft_count_total b
        ON A.tx_hash = b.tx_hash
),
token_tx_data AS (
    SELECT
        A.tx_hash,
        A.contract_address AS currency_address,
        A.to_address,
        A.from_address,
        CASE
            WHEN A.to_address = '0x5b3256965e7c3cf26e11fcaf296dfc8807c01073' THEN 'os_fee' --fee managment contract
            WHEN A.to_address = b.from_address
            OR raw_amount = MAX(raw_amount) over (
                PARTITION BY A.tx_hash
            ) THEN 'to_seller'
            ELSE 'royalty'
        END AS payment_type,
        A.raw_amount,
        A.event_index,
        ROW_NUMBER() over(
            PARTITION BY A.tx_hash
            ORDER BY
                event_index ASC
        ) AS agg_id
    FROM
        {{ ref('silver__transfers') }} A full
        OUTER JOIN (
            SELECT
                DISTINCT tx_hash,
                from_address
            FROM
                relevant_transfers
        ) b
        ON A.tx_hash = b.tx_hash
        AND A.to_address = b.from_address
    WHERE
        A.tx_hash IN (
            SELECT
                tx_hash
            FROM
                seaport_sales
        )

{% if is_incremental() %}
AND A.ingested_at >= (
    SELECT
        MAX(
            ingested_at
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
os_total_token_fees AS (
    SELECT
        tx_hash,
        SUM(raw_amount) AS os_token_fees
    FROM
        token_tx_data
    WHERE
        payment_type = 'os_fee'
    GROUP BY
        tx_hash
),
os_token_fees_per AS (
    SELECT
        A.tx_hash,
        os_token_fees / nft_count_total AS os_token_fees
    FROM
        os_total_token_fees A
        LEFT JOIN nft_count_total b
        ON A.tx_hash = b.tx_hash
),
total_token_royalties AS (
    SELECT
        tx_hash,
        SUM(raw_amount) AS royalties
    FROM
        token_tx_data
    WHERE
        payment_type = 'royalty'
    GROUP BY
        tx_hash
),
token_royalties AS (
    SELECT
        A.tx_hash,
        royalties / nft_count_total AS royalty_fee
    FROM
        total_token_royalties A
        LEFT JOIN nft_count_total b
        ON A.tx_hash = b.tx_hash
),
total_tokens_to_seller AS (
    SELECT
        tx_hash,
        SUM(raw_amount) AS sale_amount
    FROM
        token_tx_data
    WHERE
        payment_type = 'to_seller'
    GROUP BY
        tx_hash
),
tokens_to_seller AS (
    SELECT
        A.tx_hash,
        sale_amount / nft_count_total AS token_sale_amount
    FROM
        total_tokens_to_seller A
        LEFT JOIN nft_count_total b
        ON A.tx_hash = b.tx_hash
),
eth_tx_sales AS (
    SELECT
        tx_hash,
        'ETH' AS currency_address
    FROM
        seaport_sales
    WHERE
        tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                token_tx_data
        )
),
trade_currency AS (
    SELECT
        tx_hash,
        currency_address
    FROM
        token_tx_data
    UNION ALL
    SELECT
        tx_hash,
        currency_address
    FROM
        eth_tx_sales
),
tx_currency AS (
    SELECT
        DISTINCT tx_hash,
        currency_address
    FROM
        trade_currency
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
                DISTINCT LOWER(currency_address)
            FROM
                tx_currency
        )
),
token_prices AS (
    SELECT
        HOUR,
        CASE
            WHEN LOWER(token_address) IS NULL THEN 'ETH'
            ELSE LOWER(token_address)
        END AS token_address,
        AVG(price) AS price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        (
            token_address IN (
                SELECT
                    DISTINCT LOWER(currency_address)
                FROM
                    trade_currency
            )
            OR (
                token_address IS NULL
                AND symbol IS NULL
            )
        )
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                seaport_sales
        )
    GROUP BY
        HOUR,
        token_address
),
tx_data AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_number,
        to_address AS origin_to_address,
        from_address AS origin_from_address,
        eth_value,
        tx_fee,
        origin_function_signature,
        CASE
            WHEN to_address IN (
                '0x7be8076f4ea4a4ad08075c2508e481d6c946d12b',
                '0x7f268357a8c2552623316e2562d90e642bb538e5'
            ) THEN 'DIRECT'
            ELSE 'INDIRECT'
        END AS interaction_type,
        ingested_at
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                seaport_sales
        )

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
eth_prices AS (
    SELECT
        HOUR,
        token_address,
        price AS eth_price
    FROM
        token_prices
    WHERE
        token_address = 'ETH'
),
FINAL AS (
    SELECT
        b.block_number,
        b.block_timestamp,
        A.tx_hash,
        b.origin_to_address,
        b.origin_from_address,
        b.origin_function_signature,
        'sale' AS event_type,
        '0x00000000006c3852cbef3e08e8df289169ede581' AS platform_address,
        'opensea' AS platform_name,
        'seaport_1_1' AS platform_exchange_version,
        A.nft_address,
        A.project_name,
        A.from_address AS seller_address,
        A.to_address AS buyer_address,
        C.currency_address AS currency_address,
        CASE
            WHEN C.currency_address = 'ETH' THEN 'ETH'
            ELSE d.symbol
        END AS currency_symbol,
        A.tokenid,
        A.token_metadata,
        A.erc1155_value,
        A.ingested_at,
        A._log_id,
        COALESCE(
            os_fees_per_nft,
            os_token_fees,
            0
        ) AS platform_fee_unadj,
        COALESCE(
            royalty_amount,
            royalty_fee,
            0
        ) AS creator_fee_unadj,
        COALESCE(
            eth_sale_per,
            token_sale_amount
        ) AS sale_amount_unadj,
        CASE
            WHEN currency_address = 'ETH' THEN platform_fee_unadj
            WHEN currency_address <> 'ETH'
            AND d.decimals IS NULL THEN platform_fee_unadj
            ELSE platform_fee_unadj / pow(
                10,
                d.decimals
            )
        END AS platform_fee,
        CASE
            WHEN currency_address = 'ETH' THEN creator_fee_unadj
            WHEN currency_address <> 'ETH'
            AND d.decimals IS NULL THEN creator_fee_unadj
            ELSE creator_fee_unadj / pow(
                10,
                d.decimals
            )
        END AS creator_fee,
        CASE
            WHEN currency_address = 'ETH' THEN sale_amount_unadj
            WHEN currency_address <> 'ETH'
            AND d.decimals IS NULL THEN sale_amount_unadj
            ELSE sale_amount_unadj / pow(
                10,
                d.decimals
            )
        END AS sale_amount,
        ROUND(
            CASE
                WHEN d.decimals IS NOT NULL
                OR currency_address = 'ETH' THEN creator_fee * price
                ELSE NULL
            END,
            2
        ) AS creator_fee_usd,
        ROUND(
            CASE
                WHEN d.decimals IS NOT NULL
                OR currency_address = 'ETH' THEN platform_fee * price
                ELSE NULL
            END,
            2
        ) AS platform_fee_usd,
        creator_fee + platform_fee AS total_fees,
        ROUND(
            CASE
                WHEN d.decimals IS NOT NULL
                OR currency_address = 'ETH' THEN total_fees * price
                ELSE NULL
            END,
            2
        ) AS total_fees_usd,
        creator_fee + sale_amount + platform_fee AS adj_price,
        ROUND(
            CASE
                WHEN d.decimals IS NOT NULL
                OR currency_address = 'ETH' THEN adj_price * price
                ELSE NULL
            END,
            2
        ) AS price_usd,
        tx_fee,
        ROUND(
            tx_fee * eth_price,
            2
        ) AS tx_fee_usd
    FROM
        relevant_transfers A
        LEFT JOIN tx_data b
        ON A.tx_hash = b.tx_hash
        LEFT JOIN tx_currency C
        ON A.tx_hash = C.tx_hash
        LEFT JOIN decimals d
        ON C.currency_address = d.address
        LEFT JOIN os_fees_per_nft osf
        ON A.tx_hash = osf.tx_hash
        LEFT JOIN eth_sales_per
        ON A.tx_hash = eth_sales_per.tx_hash
        AND A.from_address = eth_sales_per.to_address
        LEFT JOIN eth_royalties
        ON A.tx_hash = eth_royalties.tx_hash
        LEFT JOIN os_token_fees_per
        ON A.tx_hash = os_token_fees_per.tx_hash
        LEFT JOIN token_royalties
        ON A.tx_hash = token_royalties.tx_hash
        LEFT JOIN tokens_to_seller
        ON A.tx_hash = tokens_to_seller.tx_hash
        LEFT JOIN token_prices
        ON token_prices.hour = DATE_TRUNC(
            'HOUR',
            b.block_timestamp
        )
        AND C.currency_address = token_prices.token_address
        LEFT JOIN eth_prices
        ON eth_prices.hour = DATE_TRUNC(
            'HOUR',
            b.block_timestamp
        )
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_to_address,
    origin_from_address,
    origin_function_signature,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    buyer_address,
    seller_address,
    nft_address,
    project_name,
    erc1155_value,
    tokenId,
    token_metadata,
    currency_symbol,
    currency_address,
    adj_price AS price,
    price_usd,
    total_fees,
    platform_fee,
    creator_fee,
    total_fees_usd,
    platform_fee_usd,
    creator_fee_usd,
    tx_fee,
    tx_fee_usd,
    _log_id,
    ingested_at
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    ingested_at DESC)) = 1
