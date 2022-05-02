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
    GROUP BY
        HOUR,
        token_address
)
SELECT
    block_timestamp,
    block_number,
    nft_mints.tx_hash AS tx_hash,
    event_type,
    contract_address AS nft_address,
    project_name,
    from_address AS nft_from_address,
    to_address AS nft_to_address,
    tokenId,
    erc1155_value,
    eth_value / nft_count AS mint_price_eth,
    eth_value * price AS mint_price_usd,
    nft_count,
    tx_fee,
    tx_fee * price AS tx_fee_usd
FROM
    nft_mints
    JOIN mint_price
    ON nft_mints.tx_hash = mint_price.tx_hash
    JOIN tokens_per_tx
    ON nft_mints.tx_hash = tokens_per_tx.tx_hash
    LEFT JOIN token_prices
    ON DATE_TRUNC(
        'hour',
        nft_mints.block_timestamp
    ) = token_prices.hour
