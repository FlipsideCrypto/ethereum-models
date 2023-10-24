{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform'],
  cluster_by = ['block_timestamp::DATE'],
  tags = ['non_realtime','reorg','curation']
) }}

WITH flashloans AS (

  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    aave_market AS market,
    aave_token AS protocol_token,
    flashloan_amount,
    flashloan_amount_usd,
    premium_amount,
    premium_amount_usd,
    initiator_address,
    target_address,
    aave_version AS platform,
    symbol,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__aave_flashloans') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '12 hours'
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
  spark_market AS market,
  spark_token AS protocol_token,
  flashloan_amount,
  (flashloan_amount * price) AS flashloan_amount_usd,
  premium_amount,
  (premium_amount * price) AS premium_amount_usd,
  initiator_address,
  target_address,
  platform,
  a.symbol,
  blockchain,
  a._LOG_ID,
  a._INSERTED_TIMESTAMP
FROM
  {{ ref('silver__spark_flashloans') }} a
LEFT JOIN 
  {{ ref('core__fact_hourly_token_prices') }} p
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
)
SELECT
  tx_hash,
  block_number,
  block_timestamp,
  event_index,
  origin_from_address,
  origin_to_address,
  origin_function_signature,
  contract_address,
  'FlashLoan' AS event_name,
  protocol_token as protocol_market,
  market,
  flashloan_amount,
  ROUND(flashloan_amount_usd,2) AS flashloan_amount_usd,
  premium_amount,
  premium_amount_usd,
  initiator_address,
  target_address,
  platform,
  symbol,
  blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  flashloans
