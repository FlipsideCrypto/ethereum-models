{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH x2y2_txs AS (

    SELECT
        tx_hash,
        CONCAT('0x', SUBSTR(DATA, 1115, 40)) AS nft_address,
        CONCAT('0x', SUBSTR(DATA, 27, 40)) AS to_address,
        udf_hex_to_int(SUBSTR(DATA, 1186, 33)) AS tokenid,
        ROW_NUMBER() over(
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS agg_id,
        CONCAT('0x', SUBSTR(DATA, 91, 40)) AS to_address_token
    FROM
        {{ ref('silver__logs') }}
    WHERE
        tx_status = 'SUCCESS'
        AND contract_address = '0x74312363e45dcaba76c59ec49a7aa8a65a67eed3'
        AND topics [0] = '0x3cbb63f144840e5b1b0a38a7c19211d2e89de4d7c5faf8b2d3c1776c302d1d33'

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
last_nft_transfer AS (
    SELECT
        nft.tx_hash,
        nft.contract_address,
        nft.tokenid,
        nft.to_address,
        nft.project_name,
        nft.from_address,
        nft.event_index,
        nft.token_metadata,
        nft.erc1155_value,
        nft.ingested_at,
        nft._log_id
    FROM
        {{ ref('silver__nft_transfers') }}
        nft
        INNER JOIN x2y2_txs
        ON x2y2_txs.tx_hash = nft.tx_hash
        AND x2y2_txs.nft_address = nft.contract_address
        AND x2y2_txs.tokenid = nft.tokenid qualify(ROW_NUMBER() over(PARTITION BY nft.tx_hash, nft.contract_address, nft.tokenid
    ORDER BY
        event_index DESC)) = 1
),
first_nft_transfer AS (
    SELECT
        nft.tx_hash,
        nft.contract_address,
        nft.tokenid,
        nft.to_address,
        nft.from_address AS nft_seller,
        nft.event_index,
        nft.token_metadata,
        nft.erc1155_value,
        nft.ingested_at,
        nft._log_id
    FROM
        {{ ref('silver__nft_transfers') }}
        nft
        INNER JOIN x2y2_txs
        ON x2y2_txs.tx_hash = nft.tx_hash
        AND x2y2_txs.nft_address = nft.contract_address
        AND x2y2_txs.tokenid = nft.tokenid qualify(ROW_NUMBER() over(PARTITION BY nft.tx_hash, nft.contract_address, nft.tokenid
    ORDER BY
        event_index ASC)) = 1
),
relevant_transfers AS (
    SELECT
        A.tx_hash,
        A.contract_address,
        A.project_name,
        A.tokenid,
        A.to_address AS buyer_address,
        b.nft_seller AS seller_address,
        A.event_index,
        A.token_metadata,
        A.erc1155_value,
        A.ingested_at,
        A._log_id
    FROM
        last_nft_transfer A
        JOIN first_nft_transfer b
        ON A.tx_hash = b.tx_hash
        AND A.contract_address = b.contract_address
        AND A.tokenid = b.tokenid
),
nft_base AS (
    SELECT
        tx_hash,
        contract_address,
        project_name,
        tokenid,
        buyer_address,
        seller_address,
        event_index,
        token_metadata,
        erc1155_value,
        ingested_at,
        _log_id,
        ROW_NUMBER() over(
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS agg_id
    FROM
        relevant_transfers
),
traces_base_data AS (
    SELECT
        tx_hash,
        from_address,
        to_address,
        eth_value AS amount,
        CASE
            WHEN to_address = '0xd823c605807cc5e6bd6fc0d7e4eea50d3e2d66cd' THEN 'fee' --fee managment contract
            ELSE 'other'
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
        {{ ref('silver__traces') }}
    WHERE
        eth_value > 0
        AND TYPE = 'CALL'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                x2y2_txs
        )
        AND from_address = '0x74312363e45dcaba76c59ec49a7aa8a65a67eed3' --exchange contract
),
id_sales_traces AS (
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
        ) AS sale_id
    FROM
        traces_base_data A
        INNER JOIN (
            SELECT
                DISTINCT tx_hash,
                seller_address
            FROM
                nft_base
        ) b
        ON b.tx_hash = A.tx_hash
        AND A.to_address = b.seller_address
    WHERE
        payment_type = 'other'
),
traces_group_id AS (
    SELECT
        A.*,
        b.sale_id,
        CASE
            WHEN A.level8 IS NOT NULL THEN CONCAT(
                A.level7,
                '-',
                A.level6,
                '-',
                A.level5,
                '-',
                A.level4,
                '-',
                A.level3,
                '-',
                A.level2,
                '-',
                A.level1
            ) :: STRING
            WHEN A.level7 IS NOT NULL THEN CONCAT(
                A.level6,
                '-',
                A.level5,
                '-',
                A.level4,
                '-',
                A.level3,
                '-',
                A.level2,
                '-',
                A.level1
            ) :: STRING
            WHEN A.level6 IS NOT NULL THEN CONCAT(
                A.level5,
                '-',
                A.level4,
                '-',
                A.level3,
                '-',
                A.level2,
                '-',
                A.level1
            ) :: STRING
            WHEN A.level5 IS NOT NULL THEN CONCAT(
                A.level4,
                '-',
                A.level3,
                '-',
                A.level2,
                '-',
                A.level1
            ) :: STRING
            WHEN A.level4 IS NOT NULL THEN CONCAT(
                A.level3,
                '-',
                A.level2,
                '-',
                A.level1
            ) :: STRING
            WHEN A.level3 IS NOT NULL THEN CONCAT(
                A.level2,
                '-',
                A.level1
            ) :: STRING
            WHEN A.level2 IS NOT NULL THEN A.level1 :: STRING
        END AS group_id
    FROM
        traces_base_data AS A
        LEFT JOIN id_sales_traces b
        ON A.tx_hash = b.tx_hash
        AND A.split_id = b.split_id
),
traces_agg_id AS (
    SELECT
        *,
        LAST_VALUE(sale_id) over(
            PARTITION BY tx_hash,
            group_id
            ORDER BY
                level1 ASC,
                level2 ASC,
                level3 ASC,
                level4 ASC,
                level5 ASC,
                level6 ASC,
                level7 ASC,
                level8 ASC,
                amount ASC
        ) AS agg_id
    FROM
        traces_group_id
),
traces_payment_data AS (
    SELECT
        A.tx_hash,
        A.from_address,
        A.to_address,
        A.amount,
        A.currency_symbol,
        A.currency_address,
        A.agg_id,
        b.seller_address AS nft_seller,
        CASE
            WHEN payment_type = 'fee' THEN 'platform_fee'
            WHEN payment_type = 'other'
            AND nft_seller = A.to_address THEN 'to_seller'
            WHEN payment_type = 'other'
            AND nft_seller <> A.to_address THEN 'creator_fee'
        END AS payment_type
    FROM
        traces_agg_id A
        LEFT JOIN nft_base b
        ON b.tx_hash = A.tx_hash
        AND A.agg_id = b.agg_id
),
token_transfer_data_data AS (
    SELECT
        *,
        CASE
            WHEN to_address = '0xd823c605807cc5e6bd6fc0d7e4eea50d3e2d66cd' THEN 'fee'
            ELSE 'other'
        END AS payment_type,
        SPLIT_PART(
            _log_id,
            '-',
            2
        ) AS event_index
    FROM
        {{ ref('core__ez_token_transfers') }}
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                x2y2_txs
        )
        AND from_address = '0x74312363e45dcaba76c59ec49a7aa8a65a67eed3'
),
token_transfer_agg AS (
    SELECT
        tx_hash,
        contract_address AS currency_address,
        from_address,
        to_address,
        symbol AS currency_symbol,
        amount,
        payment_type,
        event_index,
        ROW_NUMBER() over(
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS agg_id,
        CEIL(
            agg_id / 2
        ) AS join_id,
        CEIL(
            agg_id / 3
        ) AS join_id2
    FROM
        token_transfer_data_data
),
token_payment_type AS (
    SELECT
        tx_hash,
        SUM(
            CASE
                WHEN payment_type = 'fee' THEN 1
            END
        ) AS fees_paid,
        SUM(
            CASE
                WHEN payment_type = 'other' THEN 1
            END
        ) AS other_payments,
        CASE
            WHEN fees_paid = other_payments THEN 'join_id1'
            WHEN (
                fees_paid * 2
            ) = other_payments THEN 'join_id2'
            WHEN fees_paid IS NULL THEN 'agg_id'
            WHEN other_payments * 2 = fees_paid THEN 'join_id2'
        END AS join_type
    FROM
        token_transfer_agg
    GROUP BY
        tx_hash
),
token_join_type AS (
    SELECT
        A.*,
        b.join_type,
        CASE
            WHEN join_type = 'join_id1' THEN join_id
            WHEN join_type = 'join_id2' THEN join_id2
            WHEN join_type = 'agg_id' THEN agg_id
        END AS final_join_id
    FROM
        token_transfer_agg AS A
        LEFT JOIN token_payment_type AS b
        ON A.tx_hash = b.tx_hash
),
token_payment_data AS (
    SELECT
        A.tx_hash,
        A.from_address,
        A.to_address,
        A.amount,
        A.currency_symbol,
        A.currency_address,
        A.final_join_id,
        b.to_address AS nft_seller,
        b.to_address_token AS to_address_token,
        CASE
            WHEN A.currency_address = 'ETH' THEN nft_seller
            ELSE to_address_token
        END AS seller_address,
        CASE
            WHEN payment_type = 'fee' THEN 'platform_fee'
            WHEN payment_type = 'other'
            AND seller_address = A.to_address THEN 'to_seller'
            WHEN payment_type = 'other'
            AND seller_address <> A.to_address THEN 'creator_fee'
        END AS payment_type
    FROM
        token_join_type A
        LEFT JOIN x2y2_txs b
        ON b.tx_hash = A.tx_hash
        AND A.final_join_id = b.agg_id
),
all_paymemts AS (
    SELECT
        tx_hash,
        from_address,
        nft_seller,
        amount,
        currency_address,
        currency_symbol,
        payment_type,
        final_join_id
    FROM
        token_payment_data
    UNION ALL
    SELECT
        tx_hash,
        from_address,
        nft_seller,
        amount,
        currency_address,
        currency_symbol,
        payment_type,
        agg_id AS final_join_id
    FROM
        traces_payment_data
),
sale_amount AS (
    SELECT
        tx_hash,
        final_join_id,
        currency_address,
        currency_symbol,
        SUM(amount) AS sale_amount
    FROM
        all_paymemts
    GROUP BY
        tx_hash,
        final_join_id,
        currency_address,
        currency_symbol
),
platform_fees AS (
    SELECT
        tx_hash,
        final_join_id,
        currency_address,
        currency_symbol,
        SUM(amount) AS platform_fee
    FROM
        all_paymemts
    WHERE
        payment_type = 'platform_fee'
    GROUP BY
        tx_hash,
        final_join_id,
        currency_address,
        currency_symbol
),
creator_fees AS (
    SELECT
        tx_hash,
        final_join_id,
        currency_address,
        currency_symbol,
        SUM(amount) AS creator_fee
    FROM
        all_paymemts
    WHERE
        payment_type = 'creator_fee'
    GROUP BY
        tx_hash,
        final_join_id,
        currency_address,
        currency_symbol
),
transaction_data AS (
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
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                x2y2_txs
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
        AVG(price) AS token_price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        (
            token_address IN (
                SELECT
                    DISTINCT LOWER(currency_address)
                FROM
                    all_paymemts
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
                transaction_data
        )
    GROUP BY
        HOUR,
        token_address
),
eth_prices AS (
    SELECT
        HOUR,
        token_address,
        token_price AS eth_price
    FROM
        token_prices
    WHERE
        token_address = 'ETH'
),
final_nft_data AS (
    SELECT
        A.tx_hash AS tx_hash,
        t.block_number AS block_number,
        t.block_timestamp AS block_timestamp,
        A._log_id AS _log_id,
        A.ingested_at AS ingested_at,
        A.contract_address AS nft_address,
        'sale' AS event_type,
        '0x74312363e45dcaba76c59ec49a7aa8a65a67eed3' AS platform_address,
        'x2y2' AS platform_name,
        A.project_name AS project_name,
        A.seller_address AS seller_address,
        A.buyer_address AS buyer_address,
        A.tokenid AS tokenId,
        A.erc1155_value AS erc1155_value,
        A.token_metadata AS token_metadata,
        b.currency_address AS currency_address,
        b.currency_symbol AS currency_symbol,
        b.sale_amount AS price,
        COALESCE(
            d.platform_fee,
            0
        ) AS platform_fee,
        COALESCE(
            C.creator_fee,
            0
        ) AS creator_fee,
        COALESCE(
            d.platform_fee,
            0
        ) + COALESCE(
            C.creator_fee,
            0
        ) AS total_fees,
        t.origin_to_address AS origin_to_address,
        t.origin_from_address AS origin_from_address,
        t.origin_function_signature AS origin_function_signature,
        t.tx_fee AS tx_fee,
        ROUND(
            tx_fee * eth_price,
            2
        ) AS tx_fee_usd,
        ROUND(
            price * prices.token_price,
            2
        ) AS price_usd,
        ROUND(
            total_fees * prices.token_price,
            2
        ) AS total_fees_usd,
        ROUND(
            COALESCE(
                d.platform_fee,
                0
            ) * prices.token_price,
            2
        ) AS platform_fee_usd,
        ROUND(
            COALESCE(
                C.creator_fee,
                0
            ) * prices.token_price,
            2
        ) AS creator_fee_usd
    FROM
        nft_base A
        LEFT JOIN sale_amount b
        ON A.tx_hash = b.tx_hash
        AND A.agg_id = b.final_join_id
        LEFT JOIN creator_fees C
        ON A.tx_hash = C.tx_hash
        AND A.agg_id = C.final_join_id
        LEFT JOIN platform_fees d
        ON A.tx_hash = d.tx_hash
        AND A.agg_id = d.final_join_id
        LEFT JOIN transaction_data t
        ON A.tx_hash = t.tx_hash
        LEFT JOIN token_prices prices
        ON prices.hour = DATE_TRUNC(
            'HOUR',
            t.block_timestamp
        )
        AND b.currency_address = prices.token_address
        LEFT JOIN eth_prices
        ON eth_prices.hour = DATE_TRUNC(
            'HOUR',
            t.block_timestamp
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
    buyer_address,
    seller_address,
    nft_address,
    project_name,
    erc1155_value,
    tokenId,
    token_metadata,
    currency_symbol,
    currency_address,
    price,
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
    final_nft_data qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    ingested_at DESC)) = 1
