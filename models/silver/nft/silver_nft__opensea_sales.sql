{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH opensea_sales AS (

    SELECT
        _log_id,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        event_name,
        event_inputs,
        event_inputs :maker :: STRING AS seller_address,
        event_inputs :taker :: STRING AS buyer_address,
        event_inputs :price :: INTEGER AS unadj_price,
        ingested_at :: TIMESTAMP AS ingested_at
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address IN (
            '0x7be8076f4ea4a4ad08075c2508e481d6c946d12b',
            '0x7f268357a8c2552623316e2562d90e642bb538e5'
        )
        AND event_name = 'OrdersMatched'

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        )
    FROM
        {{ this }}
)
{% endif %}
),
nft_transfers AS (
    SELECT
        tx_hash,
        contract_address AS nft_address,
        from_address,
        to_address,
        tokenid,
        erc1155_value,
        ingested_at,
        _log_id
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                opensea_sales
        )

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        )
    FROM
        {{ this }}
)
{% endif %}
),
eth_tx_data AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address,
        to_address,
        eth_value,
        identifier
    FROM
        {{ ref('silver__eth_transfers') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                opensea_sales
        )

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        )
    FROM
        {{ this }}
)
{% endif %}
),
token_tx_data AS (
    SELECT
        tx_hash,
        contract_address AS currency_address,
        to_address,
        from_address,
        raw_amount
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                opensea_sales
        )

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        )
    FROM
        {{ this }}
)
{% endif %}
),
tx_data AS (
    SELECT
        tx_hash,
        to_address,
        from_address,
        eth_value,
        tx_fee,
        origin_function_signature
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                opensea_sales
        )

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        )
    FROM
        {{ this }}
)
{% endif %}
),
nfts_per_trade AS (
    SELECT
        tx_hash,
        COUNT(
            DISTINCT tokenid
        ) AS nft_count
    FROM
        nft_transfers
    GROUP BY
        tx_hash
),
eth_sales AS (
    SELECT
        tx_hash,
        'ETH' AS currency_address
    FROM
        opensea_sales
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
        eth_sales
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
                trade_currency
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
                opensea_sales
        )
    GROUP BY
        HOUR,
        token_address
)
SELECT
    opensea_sales._log_id AS _log_id,
    opensea_sales.block_number AS block_number,
    opensea_sales.block_timestamp AS block_timestamp,
    opensea_sales.tx_hash AS tx_hash,
    contract_address AS platform_address,
    event_name,
    event_inputs,
    seller_address,
    buyer_address,
    nft_address,
    tokenId,
    unadj_price,
    price AS token_price,
    nft_count,
    currency_address,
    CASE
        WHEN currency_address = 'ETH' THEN 'ETH'
        ELSE symbol
    END AS currency_symbol,
    CASE
        WHEN currency_address = 'ETH' THEN 18
        ELSE decimals
    END AS token_decimals,
    opensea_sales.ingested_at AS ingested_at,
    COALESCE(unadj_price / nft_count / pow(10, token_decimals), unadj_price) AS adj_price,
    CASE
        WHEN token_decimals IS NULL THEN NULL
        ELSE ROUND(
            adj_price * token_price,
            2
        )
    END AS price_usd
FROM
    opensea_sales
    LEFT JOIN nft_transfers
    ON opensea_sales.tx_hash = nft_transfers.tx_hash
    AND (
        (
            opensea_sales.seller_address = nft_transfers.from_address
            AND opensea_sales.buyer_address = nft_transfers.to_address
        )
        OR (
            opensea_sales.seller_address = nft_transfers.to_address
            AND opensea_sales.buyer_address = nft_transfers.from_address
        )
    )
    LEFT JOIN tx_currency
    ON tx_currency.tx_hash = opensea_sales.tx_hash
    LEFT JOIN decimals
    ON tx_currency.currency_address = decimals.address
    LEFT JOIN token_prices
    ON token_prices.hour = DATE_TRUNC(
        'HOUR',
        opensea_sales.block_timestamp
    )
    AND tx_currency.currency_address = token_prices.token_address
    LEFT JOIN nfts_per_trade
    ON nfts_per_trade.tx_hash = opensea_sales.tx_hash
WHERE
    opensea_sales.tx_hash NOT IN (
        SELECT
            tx_hash
        FROM
            aggregators
    ) -- notes
    -- might need to pull sell hash out of orders matched for txs like 0x5f5645f10b8f6340523f6fe8a596a5cfd9ef74d9e80c191652710448659c062b
ORDER BY
    tx_hash,
    tokenid
