{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH withdraws AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
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
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    spark_token AS protocol_token,
    spark_market AS withdraw_asset,
    symbol,
    withdrawn_tokens AS withdraw_amount,
    NULL AS withdraw_amount_usd,
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
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    ctoken AS protocol_token,
    CASE
        WHEN received_contract_symbol = 'ETH' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE received_contract_address
    END AS withdraw_asset,
    received_contract_symbol AS symbol,
    received_amount AS withdraw_amount,
    received_amount_usd AS withdraw_amount_usd,
    redeemer AS depositor_address,
    compound_version AS platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('silver__comp_redemptions') }}

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
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    frax_market_address AS protocol_token,
    underlying_asset AS withdraw_asset,
    underlying_symbol AS symbol,
    withdraw_amount,
    NULL AS withdraw_amount_usd,
    caller AS depositor_address,
    'Fraxlend' AS platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('silver__fraxlend_withdraws') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
FINAL AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        CASE
            WHEN platform = 'Fraxlend' THEN 'RemoveCollateral'
            WHEN platform = 'Compound V3' THEN 'WithdrawCollateral'
            WHEN platform = 'Compound V2' THEN 'Redeem'
            WHEN platform = 'Aave V1' THEN 'RedeemUnderlying'
            ELSE 'Withdraw'
        END AS event_name,
        protocol_token AS protocol_market,
        withdraw_asset,
        A.symbol AS withdraw_symbol,
        withdraw_amount,
        CASE
            WHEN platform IN (
                'Fraxlend',
                'Spark'
            ) THEN ROUND((withdraw_amount * p.price), 2)
            ELSE ROUND(
                withdraw_amount_usd,
                2
            )
        END AS withdraw_amount_usd,
        depositor_address,
        platform,
        blockchain,
        A._log_id,
        A._inserted_timestamp
    FROM
        withdraws A
        LEFT JOIN {{ ref('core__fact_hourly_token_prices') }}
        p
        ON withdraw_asset = p.token_address
        AND DATE_TRUNC(
            'hour',
            block_timestamp
        ) = p.hour
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON withdraw_asset = C.address
)
SELECT
    *
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
