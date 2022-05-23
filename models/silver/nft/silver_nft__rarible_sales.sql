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
        tx_hash
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
        SUM(eth_value) AS sale_amount
    FROM
        final_group_ids
    GROUP BY
        tx_hash,
        final_join_id
),
platform_amount_multi AS (
    SELECT
        tx_hash,
        final_join_id,
        SUM(eth_value) AS platform_fee
    FROM
        final_group_ids
    WHERE
        TYPE = 'platform_fee'
    GROUP BY
        tx_hash,
        final_join_id
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
all_new_sales AS (
    SELECT
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        project_name,
        seller_address,
        buyer_address,
        tokenid,
        erc1155_value,
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
        erc1155_value,
        sale_amount,
        platform_fee,
        creator_fee
    FROM
        multi_sales_final
)
