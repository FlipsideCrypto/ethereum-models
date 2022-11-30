{{ config(
    materialized = 'incremental',
    unique_key = 'nft_uni_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH new_rarible_tx AS (

    SELECT
        block_timestamp,
        _inserted_timestamp,
        tx_hash
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_timestamp >= '2022-09-05'
        AND contract_address = '0x9757f2d2b135150bbeb65308d4a91804107cd8d6'
        AND origin_to_address = '0x9757f2d2b135150bbeb65308d4a91804107cd8d6'
        AND topics [0] = '0x956cd63ee4cdcd81fda5f0ec7c6c36dceda99e1b412f4a650a5d26055dc3c450'

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
base_sales AS (
    SELECT
        block_timestamp,
        _inserted_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address AS nft_address,
        COALESCE (
            event_inputs :_from :: STRING,
            event_inputs :from :: STRING
        ) AS seller_address,
        COALESCE (
            event_inputs :_to :: STRING,
            event_inputs :to :: STRING
        ) AS buyer_address,
        COALESCE (
            event_inputs :_id :: STRING,
            event_inputs :tokenId :: STRING
        ) AS tokenId,
        event_inputs :_value :: STRING AS erc1155_value,
        CASE
            WHEN origin_from_address = seller_address THEN 'bid_won'
            WHEN origin_from_address = buyer_address THEN 'sale'
            ELSE 'sale'
        END AS event_type
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_timestamp >= '2022-09-05'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                new_rarible_tx
        )
        AND tx_status = 'SUCCESS'
        AND event_name IN (
            'Transfer',
            'TransferSingle'
        )
        AND origin_function_signature IN (
            '0xe99a3f80',
            '0x0d5f7d35'
        )
        AND (
            event_inputs :_id IS NOT NULL
            OR event_inputs :tokenId IS NOT NULL
        )
        AND seller_address != '0x0000000000000000000000000000000000000000'

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
eth_sales AS (
    SELECT
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        nft_address,
        seller_address,
        buyer_address,
        tokenId,
        erc1155_value,
        event_type,
        'ETH' AS currency_address,
        SUM(price_raw) AS price,
        SUM(platform_fee_raw) AS platform_fee,
        SUM(creator_fee_raw) AS creator_fee,
        platform_fee + creator_fee AS total_fees
    FROM
        (
            SELECT
                b.block_timestamp,
                t.tx_hash,
                b.origin_function_signature,
                b.origin_from_address,
                b.origin_to_address,
                b.nft_address,
                seller_address,
                buyer_address,
                tokenId,
                erc1155_value,
                event_type,
                COALESCE (
                    CASE
                        WHEN to_address = '0x1cf0df2a5a20cd61d68d4489eebbf85b8d39e18a' THEN eth_value
                    END,
                    0
                ) AS platform_fee_raw,
                CASE
                    WHEN to_address = seller_address THEN eth_value
                END AS price_raw,
                COALESCE (
                    CASE
                        WHEN to_address != seller_address
                        AND to_address != '0x1cf0df2a5a20cd61d68d4489eebbf85b8d39e18a' THEN eth_value
                    END,
                    0
                ) AS creator_fee_raw
            FROM
                {{ ref('silver__traces') }}
                t
                INNER JOIN base_sales b
                ON t.tx_hash = b.tx_hash
            WHERE
                t.block_timestamp >= '2022-09-05'
                AND t.eth_value > 0
                AND identifier != 'CALL_ORIGIN'
        )
    GROUP BY
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        nft_address,
        seller_address,
        buyer_address,
        tokenId,
        erc1155_value,
        event_type,
        currency_address
),
token_sales AS (
    SELECT
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        nft_address,
        seller_address,
        buyer_address,
        tokenId,
        erc1155_value,
        event_type,
        currency_address,
        SUM(price_raw) AS price,
        SUM(platform_fee_raw) AS platform_fee,
        SUM(creator_fee_raw) AS creator_fee,
        platform_fee + creator_fee AS total_fees
    FROM
        (
            SELECT
                b.block_timestamp,
                t.tx_hash,
                b.origin_function_signature,
                b.origin_from_address,
                b.origin_to_address,
                b.nft_address,
                seller_address,
                buyer_address,
                tokenId,
                erc1155_value,
                event_type,
                LOWER(
                    t.contract_address
                ) AS currency_address,
                COALESCE (
                    CASE
                        WHEN event_inputs :to = '0x1cf0df2a5a20cd61d68d4489eebbf85b8d39e18a' THEN event_inputs :value
                    END,
                    0
                ) AS platform_fee_raw,
                CASE
                    WHEN event_inputs :to = seller_address THEN event_inputs :value
                END AS price_raw,
                COALESCE (
                    CASE
                        WHEN event_inputs :to != seller_address
                        AND event_inputs :to != '0x1cf0df2a5a20cd61d68d4489eebbf85b8d39e18a' THEN event_inputs :value
                    END,
                    0
                ) AS creator_fee_raw
            FROM
                {{ ref('silver__logs') }}
                t
                INNER JOIN base_sales b
                ON t.tx_hash = b.tx_hash
            WHERE
                t.block_timestamp >= '2022-09-05'
                AND event_inputs :value IS NOT NULL
                AND event_name = 'Transfer'
        )
    GROUP BY
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        nft_address,
        seller_address,
        buyer_address,
        tokenId,
        erc1155_value,
        event_type,
        currency_address
),
agg_sales AS (
    SELECT
        *
    FROM
        eth_sales
    UNION ALL
    SELECT
        *
    FROM
        token_sales
),
all_prices1 AS (
    SELECT
        HOUR,
        decimals,
        CASE
            WHEN symbol IS NULL
            AND token_address IS NULL THEN 'ETH'
            ELSE symbol
        END AS symbol,
        CASE
            WHEN LOWER(token_address) IS NULL THEN 'ETH'
            ELSE LOWER(token_address)
        END AS currency_address,
        AVG(price) AS price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        (
            currency_address IN (
                SELECT
                    DISTINCT currency_address
                FROM
                    eth_sales
                UNION
                SELECT
                    DISTINCT currency_address
                FROM
                    token_sales
            )
            OR (
                token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            )
        )
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                base_sales
        )
    GROUP BY
        HOUR,
        decimals,
        symbol,
        token_address
),
all_prices AS (
    SELECT
        HOUR,
        decimals,
        symbol,
        currency_address,
        price
    FROM
        all_prices1
    UNION ALL
    SELECT
        HOUR,
        18 AS decimals,
        'ETH' AS symbol,
        'ETH' AS currency_address,
        price
    FROM
        all_prices1
    WHERE
        currency_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
),
eth_price AS (
    SELECT
        HOUR,
        AVG(price) AS eth_price_hourly
    FROM
        all_prices
    WHERE
        currency_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    GROUP BY
        HOUR
),
agg_sales_prices AS (
    SELECT
        t.block_number,
        t.block_timestamp,
        s.tx_hash,
        s.event_type,
        s.origin_to_address AS platform_address,
        'rarible' AS platform_name,
        'rarible' AS platform_exchange_version,
        seller_address,
        buyer_address,
        s.nft_address,
        n.project_name,
        s.erc1155_value,
        s.tokenId,
        n.token_metadata,
        p.symbol AS currency_symbol,
        s.currency_address,
        CASE
            WHEN s.currency_address = 'ETH' THEN s.price
            WHEN s.currency_address != 'ETH'
            AND p.currency_address IS NOT NULL THEN s.price / pow(
                10,
                decimals
            )
            WHEN p.currency_address IS NULL THEN s.price
        END AS prices,
        prices * p.price AS price_usd,
        CASE
            WHEN s.currency_address = 'ETH' THEN total_fees
            WHEN s.currency_address != 'ETH'
            AND p.currency_address IS NOT NULL THEN total_fees / pow(
                10,
                decimals
            )
            WHEN p.currency_address IS NULL THEN total_fees
        END AS total_fees_adj,
        CASE
            WHEN s.currency_address = 'ETH' THEN platform_fee
            WHEN s.currency_address != 'ETH'
            AND p.currency_address IS NOT NULL THEN platform_fee / pow(
                10,
                decimals
            )
            WHEN p.currency_address IS NULL THEN platform_fee
        END AS platform_fee_adj,
        CASE
            WHEN s.currency_address = 'ETH' THEN creator_fee
            WHEN s.currency_address != 'ETH'
            AND p.currency_address IS NOT NULL THEN creator_fee / pow(
                10,
                decimals
            )
            WHEN p.currency_address IS NULL THEN creator_fee
        END AS creator_fee_adj,
        total_fees_adj * p.price AS total_fees_usd,
        platform_fee_adj * p.price AS platform_fee_usd,
        creator_fee_adj * p.price AS creator_fee_usd,
        prices + total_fees_adj AS total_transaction_price,
        price_usd + total_fees_usd AS total_transaction_price_usd,
        t.tx_fee,
        t.tx_fee * e.eth_price_hourly AS tx_fee_usd,
        s.origin_from_address,
        s.origin_to_address,
        s.origin_function_signature,
        CONCAT(
            s.tx_hash,
            '-',
            s.tokenId,
            '-',
            COALESCE(
                s.erc1155_value,
                0
            )
        ) AS nft_uni_id,
        t._inserted_timestamp,
        t.input_data
    FROM
        agg_sales s
        INNER JOIN {{ ref('silver__transactions') }}
        t
        ON t.tx_hash = s.tx_hash
        LEFT JOIN all_prices p
        ON DATE_TRUNC(
            'hour',
            t.block_timestamp
        ) = p.hour
        AND s.currency_address = p.currency_address
        LEFT JOIN {{ ref('silver__nft_transfers') }}
        n
        ON n.tx_hash = s.tx_hash
        AND n.contract_address = s.nft_address
        LEFT JOIN eth_price e
        ON DATE_TRUNC(
            'hour',
            t.block_timestamp
        ) = e.hour
    WHERE
        t.block_number IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY nft_uni_id
    ORDER BY
        price_usd DESC)) = 1
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address,
    project_name,
    erc1155_value,
    tokenId,
    token_metadata,
    currency_symbol,
    currency_address,
    total_transaction_price AS price,
    total_transaction_price_usd AS price_usd,
    total_fees_adj AS total_fees,
    platform_fee_adj AS platform_fee,
    creator_fee_adj AS creator_fee,
    total_fees_usd,
    platform_fee_usd,
    creator_fee_usd,
    tx_fee,
    tx_fee_usd,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    nft_uni_id,
    _inserted_timestamp,
    input_data
FROM
    agg_sales_prices
