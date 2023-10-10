{{ config(
  materialized = 'incremental',
  unique_key = "_log_id",
  cluster_by = ['block_timestamp::DATE'],
  tags = ['non_realtime']
) }}

WITH flashloans AS (

  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
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
    {{ ref('silver__aave_ez_flashloans') }}

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
  spark_market AS market,
  spark_token AS protocol_token,
  flashloan_amount,
  flashloan_amount_usd,
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
  {{ ref('silver__spark_ez_flashloans') }}

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
  protocol_token,
  market,
  flashloan_amount,
  flashloan_amount_usd,
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
