{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

WITH nft_mints AS (

    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        project_name,
        from_address,
        to_address,
        tokenId,
        erc1155_value,
        'nft_mint' AS event_type,
        ingested_at
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        event_type = 'mint'
),
mint_price AS (
    SELECT
        tx_hash,
        eth_value,
        tx_fee
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        status = 'SUCCESS'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                nft_mints
        )
),
tokens_per_tx AS (
    SELECT
        tx_hash,
        COUNT(
            DISTINCT tokenId
        ) AS nft_count
    FROM
        nft_mints
    GROUP BY
        tx_hash
),
tokens_moved AS (
    SELECT
        tx_hash,
        _log_id,
        from_address,
        to_address,
        contract_address,
        symbol,
        amount,
        amount_usd
    FROM
        {{ ref('core__ez_token_transfers') }}
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                nft_mints
        ) qualify(ROW_NUMBER() over(PARTITION BY tx_hash
    ORDER BY
        amount_usd DESC)) = 1
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
            token_address IS NULL
            AND symbol IS NULL
        )
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                nft_mints
        )
        OR LOWER(token_address) IN (
            SELECT
                DISTINCT contract_address
            FROM
                tokens_moved
        )
    GROUP BY
        HOUR,
        token_address
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
)
SELECT
    block_timestamp,
    block_number,
    nft_mints.tx_hash AS tx_hash,
    event_type,
    nft_mints.contract_address AS nft_address,
    project_name,
    nft_mints.from_address AS nft_from_address,
    nft_mints.to_address AS nft_to_address,
    tokenId,
    erc1155_value,
    eth_value / nft_count AS mint_price_eth,
    ROUND(
        eth_value * eth_price,
        2
    ) AS mint_price_usd,
    nft_count,
    tokens_moved.amount / nft_count AS mint_price_tokens,
    ROUND(
        tokens_moved.amount_usd / nft_count,
        2
    ) AS mint_price_tokens_usd,
    tokens_moved.symbol AS mint_token_symbol,
    tokens_moved.contract_address AS mint_token_address,
    tx_fee,
    ROUND(
        tx_fee * eth_price,
        2
    ) AS tx_fee_usd
FROM
    nft_mints
    JOIN mint_price
    ON nft_mints.tx_hash = mint_price.tx_hash
    JOIN tokens_per_tx
    ON nft_mints.tx_hash = tokens_per_tx.tx_hash
    LEFT JOIN tokens_moved
    ON tokens_moved.tx_hash = nft_mints.tx_hash
    LEFT JOIN token_prices
    ON DATE_TRUNC(
        'hour',
        nft_mints.block_timestamp
    ) = token_prices.hour
    AND token_prices.token_address = tokens_moved.contract_address
    LEFT JOIN eth_prices
    ON DATE_TRUNC(
        'hour',
        nft_mints.block_timestamp
    ) = eth_prices.hour
