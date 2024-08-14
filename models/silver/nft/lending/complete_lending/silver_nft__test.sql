{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number', 'platform_exchange_version'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH base_models AS (

    SELECT
        block_number,
        block_timestamp,
        platform_exchange_version,
        tx_hash,
        loanId,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_nft__nftfi_v2_liquidations') }}

{% if is_incremental() and 'nftfi_v2' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    platform_exchange_version,
    tx_hash,
    loanId,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver_nft__nftfi_v1_liquidations') }}

{% if is_incremental() and 'nftfi_v1' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    platform_exchange_version,
    tx_hash,
    loanId,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver_nft__arcade_v1_liquidations') }}

{% if is_incremental() and 'arcade_v1' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
tx_data AS (
    SELECT
        tx_hash,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        origin_function_signature,
        tx_fee
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        block_timestamp :: DATE >= '2020-05-01'
        AND block_timestamp :: DATE IN (
            SELECT
                block_timestamp :: DATE
            FROM
                base_models
        )

{% if is_incremental() and 'transactions' not in var('HEAL_MODELS') %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
        {{ this }}
)
{% endif %}
),
eth_price AS (
    SELECT
        HOUR,
        price AS eth_price_hourly
    FROM
        {{ ref('price__ez_prices_hourly') }}
    WHERE
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        platform_exchange_version,
        tx_hash,
        loanId,
        tx_fee,
        tx_fee * eth_price_hourly AS tx_fee_usd,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        _log_id,
        _inserted_timestamp,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        base_models b
        INNER JOIN tx_data USING (tx_hash)
        LEFT JOIN eth_price e
        ON DATE_TRUNC(
            'hour',
            b.block_timestamp
        ) = e.hour

{% if is_incremental() and 'heal_tx' in var('HEAL_MODELS') %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    platform_exchange_version,
    tx_hash,
    loanId,
    f.tx_fee,
    f.tx_fee * eth_price_hourly AS tx_fee_usd,
    f.origin_from_address,
    f.origin_to_address,
    f.origin_function_signature,
    _log_id,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _invocation_id
FROM
    {{ this }}
    t
    INNER JOIN tx_data f USING (
        tx_hash
    )
    LEFT JOIN eth_price e
    ON DATE_TRUNC(
        'hour',
        t.block_timestamp
    ) = e.hour
WHERE
    t.tx_fee IS NULL
{% endif %}
)
SELECT
    *
FROM
    FINAL
