{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH nft_mints AS (

    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        from_address,
        to_address,
        tokenId,
        'nft_mint' AS event_type,
        ingested_at
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        from_address = '0x0000000000000000000000000000000000000000'

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
)
SELECT
    _log_id,
    block_number,
    nft_mints.tx_hash AS tx_hash,
    block_timestamp,
    contract_address,
    from_address,
    to_address,
    tokenId,
    event_type,
    eth_value / nft_count AS eth_value,
    nft_count,
    tx_fee,
    ingested_at
FROM
    nft_mints
    JOIN mint_price
    ON nft_mints.tx_hash = mint_price.tx_hash
    JOIN tokens_per_tx
    ON nft_mints.tx_hash = tokens_per_tx.tx_hash qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    ingested_at DESC)) = 1
