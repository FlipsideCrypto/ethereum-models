{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg','curation']
) }}

with prices AS (
  SELECT
    HOUR,
    token_address,
    price
  FROM
    {{ ref('core__fact_hourly_token_prices') }}
  WHERE
    token_address IN (
      SELECT
        DISTINCT underlying_asset
      FROM
        {{ ref('silver__fraxlend_asset_details') }}
      UNION
          SELECT
        DISTINCT underlying_address
      FROM
        {{ ref('silver__spark_tokens') }}
    )

{% if is_incremental() %}
AND HOUR >= (
  SELECT
    MAX(_inserted_timestamp) - INTERVAL '36 hours'
  FROM
    {{ this }}
)
{% endif %}
),
withdraws AS (

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
    (withdrawn_tokens * price) AS withdraw_amount_usd,
    depositor_address,
    platform,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('silver__spark_withdraws') }}

LEFT JOIN 
    prices p
ON 
    spark_market = p.token_address
  AND DATE_TRUNC(
    'hour',
    block_timestamp
  ) = p.hour

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
    ctoken AS protocol_token,
    CASE
        WHEN received_contract_symbol = 'ETH' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE received_contract_address
    END AS withdraw_asset,
    received_contract_symbol AS symbol,
    received_amount AS withdraw_amount,
    received_amount_usd AS withdraw_amount_usd,
    redeemer AS depositor_address,
    compound_version AS protocol,
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
  frax_market_address AS protocol_token,
  underlying_asset as withdraw_asset,
  underlying_symbol AS symbol,
  withdraw_amount,
  (withdraw_amount * price) AS withdraw_amount_usd,
  caller AS depositor_address,
  'Fraxlend' AS platform,
  'ethereum' AS blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__fraxlend_withdraws') }}
LEFT JOIN 
    prices p
ON 
    underlying_asset = p.token_address
  AND DATE_TRUNC(
    'hour',
    block_timestamp
  ) = p.hour

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
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    CASE 
      WHEN platform = 'Fraxlend' THEN 'RemoveCollateral'
      WHEN platform = 'Compound V3' THEN 'WithdrawCollateral'
      WHEN platform = 'Compound V2' THEN 'Redeem'
      WHEN platform = 'Aave V1' THEN 'RedeemUnderlying'
      ELSE 'Withdraw'
    END AS event_name,
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
