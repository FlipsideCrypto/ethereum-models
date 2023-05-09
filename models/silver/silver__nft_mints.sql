{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['core']
) }}

WITH nft_mints AS (

    SELECT
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        project_name,
        from_address,
        to_address,
        tokenId,
        token_metadata,
        erc1155_value,
        'nft_mint' AS event_type,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        event_type = 'mint'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
mint_price AS (
    SELECT
        tx_hash,
        VALUE AS eth_value,
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

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
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
        raw_amount
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                nft_mints
        ) qualify(ROW_NUMBER() over(PARTITION BY tx_hash
    ORDER BY
        raw_amount DESC)) = 1
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
                    DISTINCT LOWER(contract_address)
                FROM
                    tokens_moved
            )
            OR (
                token_address IS NULL
                AND symbol IS NULL
            )
            OR token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
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
),
metadata AS (
    SELECT
        LOWER(address) AS address,
        symbol,
        NAME,
        decimals
    FROM
        {{ ref('core__dim_contracts') }}
    WHERE
        decimals IS NOT NULL
),
eth_prices AS (
    SELECT
        HOUR,
        token_address,
        price AS eth_price
    FROM
        token_prices
    WHERE
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
),
FINAL AS (
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
        token_metadata,
        erc1155_value,
        eth_value / nft_count AS mint_price_eth,
        ROUND(
            eth_value / nft_count * eth_price,
            2
        ) AS mint_price_usd,
        nft_count,
        raw_amount,
        CASE
            WHEN metadata.decimals IS NOT NULL THEN raw_amount / pow(
                10,
                metadata.decimals
            )
            ELSE raw_amount
        END AS amount,
        CASE
            WHEN metadata.decimals IS NOT NULL THEN amount * price
            ELSE NULL
        END AS amount_usd,
        amount / nft_count AS mint_price_tokens,
        ROUND(
            amount_usd / nft_count,
            2
        ) AS mint_price_tokens_usd,
        symbol AS mint_token_symbol,
        tokens_moved.contract_address AS mint_token_address,
        tx_fee,
        ROUND(
            tx_fee * eth_price,
            2
        ) AS tx_fee_usd,
        nft_mints._inserted_timestamp,
        nft_mints._log_id
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
        LEFT JOIN metadata
        ON metadata.address = tokens_moved.contract_address
    WHERE
        nft_count > 0
)
SELECT
    block_timestamp,
    block_number,
    tx_hash,
    event_type,
    nft_address,
    project_name,
    nft_from_address,
    nft_to_address,
    tokenId,
    token_metadata,
    erc1155_value,
    mint_price_eth,
    mint_price_usd,
    nft_count,
    amount,
    amount_usd,
    mint_price_tokens,
    mint_price_tokens_usd,
    mint_token_symbol,
    mint_token_address,
    tx_fee,
    _log_id,
    _inserted_timestamp
FROM
    FINAL
