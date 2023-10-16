{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg']
) }}

WITH withdraws AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        aave_token AS protocol_token,
        aave_market AS withdraw_asset,
        symbol,
        withdrawn_tokens AS withdraw_amount,
        withdrawn_usd AS withdraw_amount_usd,
        depositor_address,
        aave_version AS platform,
        blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__aave_withdraws') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    spark_token AS protocol_token,
    spark_market AS withdraw_asset,
    symbol,
    withdrawn_tokens AS withdraw_amount,
    withdrawn_usd AS withdraw_amount_usd,
    depositor_address,
    platform,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('silver__spark_withdraws') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    compound_market AS protocol_token,
    withdraw_asset,
    withdraw_symbol as symbol,
    withdrawn_tokens AS withdraw_amount,
    withdrawn_usd AS withdraw_amount_usd,
    depositor_address,
    'Compound V3' AS platform,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('silver__compv3_withdraws') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    ctoken AS protocol_token,
    CASE
        WHEN received_contract_symbol = 'ETH' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE received_contract_address
    END AS withdraw_asset,
    received_contract_symbol AS symbol,
    received_amount AS withdraw_amount,
    received_amount_usd AS withdraw_amount_usd,
    redeemer AS depositor_address,
    'Compound V2' AS platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('silver__compv2_redemptions') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    protocol_token AS protocol_market,
    withdraw_asset,
    symbol AS withdraw_symbol,
    withdraw_amount,
    withdraw_amount_usd,
    depositor_address,
    platform,
    blockchain,
    _log_id,
    _inserted_timestamp
FROM
    withdraws
