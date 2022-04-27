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
        from_address
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
)
SELECT
    _log_id,
    block_number,
    block_timestamp,
    opensea_sales.tx_hash AS tx_hash,
    contract_address,
    event_name,
    event_inputs,
    seller_address,
    buyer_address,
    unadj_price,
    currency_address,
    ingested_at
FROM
    opensea_sales
    LEFT JOIN trade_currency
    ON trade_currency.tx_hash = opensea_sales.tx_hash
