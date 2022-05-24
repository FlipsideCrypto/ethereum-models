{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH rarible_sales AS (

    SELECT
        _log_id,
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        ingested_at,
        contract_address,
        CONCAT('0x', SUBSTR(DATA, 91, 40)) AS from_address,
        CONCAT('0x', SUBSTR(DATA, 155, 40)) AS to_address,
        CASE
            WHEN CONCAT('0x', RIGHT(DATA, 40)) = '0x0000000000000000000000000000000000000000' THEN 'ETH'
            ELSE CONCAT('0x', RIGHT(DATA, 40))
        END AS currency_address,
        CASE
            WHEN SUBSTR(
                DATA,
                195,
                10
            ) = '1a0388dd00' THEN 'sale'
            ELSE 'bid_won'
        END AS trade_direction,
        silver.js_hex_to_int(SUBSTR(DATA, 433, 18)) / pow(
            10,
            18
        ) AS eth_value,
        ROW_NUMBER() over(
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS nft_log_id
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address = LOWER('0x9757F2d2b135150BBeb65308D4a91804107cd8D6')
        AND topics [0] :: STRING = '0xcae9d16f553e92058883de29cb3135dbc0c1e31fd7eace79fef1d80577fe482e'
        AND eth_value >.0000001
        AND tx_status = 'SUCCESS'

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
nft_transfers AS (
    SELECT
        *,
        ROW_NUMBER() over(
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS agg_id
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                base_data
        )
        AND from_address IN (
            SELECT
                DISTINCT to_address
            FROM
                base_data
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
nft_sellers AS (
    nft_sellers AS (
        SELECT
            DISTINCT tx_hash,
            from_address
        FROM
            nft_transfers
    )
),
payment_type AS (
    SELECT
        b.*,
        CASE
            WHEN b.to_address = LOWER('0x1cf0dF2A5A20Cd61d68d4489eEBbf85b8d39e18a') THEN 'platform_fee'
            WHEN nft_sellers.from_address IS NOT NULL THEN 'to_seller'
            ELSE 'creator_fee'
        END AS TYPE,CASE
            WHEN TYPE = 'to_seller' THEN CEIL(
                nft_log_id / 3
            )
            WHEN TYPE = 'creator_fee' THEN CEIL(
                nft_log_id / 3
            )
            WHEN TYPE = 'platform_fee' THEN CEIL(
                nft_log_id / 3
            )
        END AS joinid,
        CASE
            WHEN TYPE = 'to_seller' THEN CEIL(
                nft_log_id / 4
            )
            WHEN TYPE = 'creator_fee' THEN CEIL(
                nft_log_id / 4
            )
            WHEN TYPE = 'platform_fee' THEN CEIL(
                nft_log_id / 4
            )
        END AS joinid2,
        CASE
            WHEN TYPE = 'to_seller' THEN CEIL(
                nft_log_id / 2
            )
            WHEN TYPE = 'creator_fee' THEN CEIL(
                nft_log_id / 2
            )
            WHEN TYPE = 'platform_fee' THEN CEIL(
                nft_log_id / 2
            )
        END AS joinid3
    FROM
        base_data b
        LEFT JOIN nft_sellers
        ON nft_sellers.tx_hash = b.tx_hash
        AND nft_sellers.from_address = b.to_address
),
multi_sales AS (
    SELECT
        tx_hash,
        COUNT(*)
    FROM
        nft_transfers
    GROUP BY
        tx_hash
    HAVING
        COUNT(*) > 1
),
sale_amount_basic AS (
    SELECT
        tx_hash,
        currency_address,
        trade_direction,
        SUM(eth_value) AS sale_amount
    FROM
        payment_type
    WHERE
        tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                multi_sales
        )
    GROUP BY
        tx_hash,
        currency_address,
        trade_direction
),
platform_amount_basic AS (
    SELECT
        tx_hash,
        SUM(eth_value) AS platform_fee
    FROM
        payment_type
    WHERE
        tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                multi_sales
        )
        AND TYPE = 'platform_fee'
    GROUP BY
        tx_hash
),
creator_amount_basic AS (
    SELECT
        tx_hash,
        SUM(eth_value) AS creator_fee
    FROM
        payment_type
    WHERE
        tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                multi_sales
        )
        AND TYPE = 'creator_fee'
    GROUP BY
        tx_hash
),
basic_join AS (
    SELECT
        b.block_number,
        b.tx_hash,
        b.block_timestamp,
        b.contract_address,
        b.project_name,
        b.from_address AS seller_address,
        b.to_address AS buyer_address,
        b.tokenid,
        b.erc1155_value,
        b.token_metadata AS token_metadata,
        currency_address,
        trade_direction,
        sale_amount,
        platform_fee,
        creator_fee
    FROM
        nft_transfers b
        INNER JOIN sale_amount_basic
        ON b.tx_hash = sale_amount_basic.tx_hash
        LEFT JOIN platform_amount_basic
        ON b.tx_hash = platform_amount_basic.tx_hash
        LEFT JOIN creator_amount_basic
        ON b.tx_hash = creator_amount_basic.tx_hash
),
multi_sales_types AS (
    SELECT
        tx_hash,
        SUM(
            CASE
                WHEN TYPE = 'platform_fee' THEN 1
            END
        ) AS platform_fee,
        SUM(
            CASE
                WHEN TYPE = 'to_seller' THEN 1
            END
        ) AS to_seller,
        SUM(
            CASE
                WHEN TYPE = 'creator_fee' THEN 1
            END
        ) AS creator_fee,
        CASE
            WHEN platform_fee = to_seller
            AND to_seller = creator_fee THEN 'joinid'
            WHEN (
                to_seller * 2
            ) = platform_fee
            AND to_seller = creator_fee THEN 'joinid2'
            WHEN (
                platform_fee * 2
            ) = to_seller
            AND creator_fee IS NULL THEN 'joinid'
            WHEN to_seller = creator_fee
            AND platform_fee IS NULL THEN 'joinid'
            WHEN to_seller IS NOT NULL
            AND platform_fee IS NULL
            AND creator_fee IS NULL THEN 'atb_id'
            WHEN platform_fee = to_seller
            AND creator_fee IS NULL THEN 'joinid3'
            WHEN (
                to_seller * 2
            ) = platform_fee
            AND creator_fee IS NULL THEN 'joinid'
        END AS join_type
    FROM
        payment_type
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                multi_sales
        )
    GROUP BY
        tx_hash
),
final_group_ids AS (
    SELECT
        b.*,
        join_type,
        CASE
            WHEN join_type = 'joinid' THEN joinid
            WHEN join_type = 'joinid2' THEN joinid2
            WHEN join_type = 'joinid3' THEN joinid3
            WHEN join_type = 'atb_id' THEN atb_id
        END AS final_join_id
    FROM
        payment_type b
        LEFT JOIN multi_sales_types
        ON b.tx_hash = multi_sales_types.tx_hash
    WHERE
        b.tx_hash IN (
            SELECT
                tx_hash
            FROM
                multi_sales
        )
),
sale_amount_multi AS (
    SELECT
        tx_hash,
        final_join_id,
        currency_address,
        SUM(eth_value) AS sale_amount
    FROM
        final_group_ids
    GROUP BY
        tx_hash,
        final_join_id,
        currency_address
),
platform_amount_multi AS (
    SELECT
        tx_hash,
        final_join_id,
        trade_direction,
        SUM(eth_value) AS platform_fee
    FROM
        final_group_ids
    WHERE
        TYPE = 'platform_fee'
    GROUP BY
        tx_hash,
        final_join_id,
        trade_direction
),
creator_amount_multi AS (
    SELECT
        tx_hash,
        final_join_id,
        SUM(eth_value) AS creator_fee
    FROM
        final_group_ids
    WHERE
        TYPE = 'creator_fee'
    GROUP BY
        tx_hash,
        final_join_id
),
multi_sales_final AS (
    SELECT
        b.block_number AS block_number,
        b.tx_hash AS tx_hash,
        b.block_timestamp AS block_timestamp,
        b.contract_address AS contract_address,
        b.project_name AS project_name,
        b.from_address AS seller_address,
        b.to_address AS buyer_address,
        b.tokenid AS tokenid,
        b.erc1155_value AS erc1155_value,
        b.token_metadata AS token_metadata,
        currency_address,
        trade_direction,
        sale_amount AS sale_amount,
        platform_fee AS platform_fee,
        creator_fee AS creator_fee
    FROM
        nft_transfers b
        INNER JOIN sale_amount_multi
        ON b.tx_hash = sale_amount_multi.tx_hash
        AND b.agg_id = sale_amount_multi.final_join_id
        LEFT JOIN platform_amount_multi
        ON b.tx_hash = platform_amount_multi.tx_hash
        AND b.agg_id = platform_amount_multi.final_join_id
        LEFT JOIN creator_amount_multi
        ON b.tx_hash = creator_amount_multi.tx_hash
        AND b.agg_id = creator_amount_multi.final_join_id
    WHERE
        b.tx_hash IN (
            SELECT
                tx_hash
            FROM
                multi_sales
        )
),
legacy_exchange_txs AS (
    SELECT
        tx_hash,
        CASE
            WHEN event_inputs :buyValue :: FLOAT > event_inputs :sellValue :: FLOAT THEN 'bid_won'
            ELSE 'sale'
        END AS trade_direction
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address = LOWER('0xcd4EC7b66fbc029C116BA9Ffb3e59351c20B5B06')
        AND event_name = 'Buy'
        AND tx_status = 'SUCCESS'
        AND block_number < 14000000

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
legacy_nft_transfers AS (
    SELECT
        *
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                legacy_exchange_txs
        )
        AND block_number < 14000000

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
legacy_token_transfers AS (
    SELECT
        tx_hash,
        from_address,
        to_address,
        contract_address,
        symbol,
        amount
    FROM
        {{ ref('core__ez_token_transfers') }}
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                legacy_exchange_txs
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
legacy_traces AS (
    SELECT
        tx_hash,
        from_address,
        to_address,
        'ETH' AS contract_address,
        'ETH' AS symbol,
        eth_value AS amount
    FROM
        {{ ref('silver__eth_transfers') }}
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                legacy_exchange_txs
        )
        AND identifier <> 'CALL_ORIGIN'

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
all_legacy_sales AS (
    SELECT
        tx_hash,
        from_address,
        to_address,
        contract_address,
        symbol,
        amount
    FROM
        legacy_token_transfers
    UNION ALL
    SELECT
        tx_hash,
        from_address,
        to_address,
        contract_address,
        symbol,
        amount
    FROM
        legacy_traces
),
legacy_sales_amount AS (
    SELECT
        tx_hash,
        contract_address AS currency_address,
        symbol,
        SUM(amount) AS sale_amount
    FROM
        all_legacy_sales
    GROUP BY
        tx_hash,
        contract_address,
        symbol
),
legacy_platform_fees AS (
    SELECT
        tx_hash,
        contract_address AS currency_address,
        symbol,
        SUM(amount) AS platform_fee
    FROM
        all_legacy_sales
    WHERE
        to_address = LOWER('0xe627243104A101Ca59a2c629AdbCd63a782E837f')
    GROUP BY
        tx_hash,
        contract_address,
        symbol
),
legacy_creator_fees AS (
    SELECT
        tx_hash,
        contract_address AS currency_address,
        symbol,
        SUM(amount) AS creator_fee
    FROM
        all_legacy_sales
    WHERE
        to_address <> LOWER('0xe627243104A101Ca59a2c629AdbCd63a782E837f')
        AND to_address NOT IN (
            SELECT
                DISTINCT from_address
            FROM
                legacy_nft_transfers
        )
    GROUP BY
        tx_hash,
        contract_address,
        symbol
),
final_legacy_table AS (
    SELECT
        b.block_number AS block_number,
        b.tx_hash AS tx_hash,
        b.block_timestamp AS block_timestamp,
        b.contract_address AS contract_address,
        b.project_name AS project_name,
        b.from_address AS seller_address,
        b.to_address AS buyer_address,
        b.tokenid AS tokenid,
        b.erc1155_value AS erc1155_value,
        s.currency_address AS currency_address,
        b.token_metadata AS token_metadata,
        trade_direction,
        sale_amount AS sale_amount,
        platform_fee AS platform_fee,
        creator_fee AS creator_fee
    FROM
        legacy_nft_transfers b
        LEFT JOIN legacy_sales_amount s
        ON b.tx_hash = s.tx_hash
        LEFT JOIN legacy_platform_fees
        ON b.tx_hash = s.tx_hash
        LEFT JOIN legacy_creator_fees
        ON b.tx_hash = s.tx_hash
        LEFT JOIN legacy_exchange_txs
        ON legacy_exchange_txs.tx_hash = b.tx_hash
),
all_sales AS (
    SELECT
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        project_name,
        seller_address,
        buyer_address,
        tokenid,
        token_metadata,
        erc1155_value,
        currency_address,
        trade_direction,
        sale_amount,
        platform_fee,
        creator_fee
    FROM
        basic_join
    UNION ALL
    SELECT
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        project_name,
        seller_address,
        buyer_address,
        tokenid,
        token_metadata,
        erc1155_value,
        currency_address,
        trade_direction,
        sale_amount,
        platform_fee,
        creator_fee
    FROM
        multi_sales_final
    UNION ALL
    SELECT
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        project_name,
        seller_address,
        buyer_address,
        tokenid,
        token_metadata,
        erc1155_value,
        currency_address,
        trade_direction,
        sale_amount,
        platform_fee,
        creator_fee
    FROM
        final_legacy_table
),
tx_data AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_number,
        to_address,
        from_address,
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
                all_sales
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
                    all_sales
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
                all_sales
        )
    GROUP BY
        HOUR,
        token_address
) -- add final join
-- add tests and docs
