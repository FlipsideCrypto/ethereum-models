{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime'],
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
        {{ ref('silver__aave_ez_withdraws') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) :: DATE
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
        {{ ref('silver__spark_ez_withdraws') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) :: DATE
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
    asset AS withdraw_asset,
    symbol,
    withdraw_tokens AS withdraw_amount,
    withdrawn_usd AS withdraw_amount_usd,
    depositor_address,
    compound_version AS platform,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('silver__compv3_ez_withdraws') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) :: DATE
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
    received_contract_address AS withdraw_asset,
    received_contract_symbol,
    received_amount AS withdraw_amount,
    received_amount_usd AS withdraw_amount_usd,
    redeemer AS depositor_address,
    'Comp V2' AS platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('compound__ez_redemptions') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) :: DATE
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    *
FROM
    withdraws
