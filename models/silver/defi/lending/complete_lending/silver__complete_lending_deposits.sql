{{ config(
  materialized = 'incremental',
  unique_key = "_log_id",
  cluster_by = ['block_timestamp::DATE'],
  tags = ['non_realtime']
) }}

WITH deposits AS (

  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    aave_market AS deposit_asset,
    aave_token AS market,
    issued_tokens AS deposit_amount,
    supplied_usd AS deposit_amount_usd,
    depositor_address,
    lending_pool_contract,
    NULL AS issued_deposit_tokens,
    aave_version AS platform,
    symbol,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__aave_ez_deposits') }}

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
  spark_market AS deposit_asset,
  spark_token AS market,
  issued_tokens AS deposit_amount,
  supplied_usd AS deposit_amount_usd,
  depositor_address,
  lending_pool_contract,
  NULL AS issued_deposit_tokens,
  platform,
  symbol,
  blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__spark_ez_deposits') }}

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
  deposit_asset,
  compound_market AS market,
  supply_tokens AS deposit_amount,
  supply_usd AS deposit_amount_usd,
  depositor_address,
  NULL AS lending_pool_contract,
  NULL AS issued_deposit_tokens,
  compound_version AS platform,
  symbol,
  blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__compv3_ez_deposits') }}

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
  CASE
    WHEN supplied_symbol = 'ETH' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    ELSE supplied_contract_addr 
  END AS deposit_asset,
  ctoken AS market,
  supplied_base_asset AS deposit_amount,
  supplied_base_asset_usd AS deposit_amount_usd,
  supplier AS depositor_address,
  NULL AS lending_pool_contract,
  issued_ctokens AS issued_deposit_tokens,
  'Compound V2' AS platform,
  supplied_symbol AS symbol,
  'ethereum' AS blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__compv2_ez_deposits') }}

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
  deposit_asset,
  frax_market_address AS market,
  deposit_amount AS deposit_amount,
  deposit_amount_usd AS deposit_amount_usd,
  caller AS depositor_address,
  NULL AS lending_pool_contract,
  deposit_shares AS issued_deposit_tokens,
  'Fraxlend' AS platform,
  frax_market_symbol AS symbol,
  'ethereum' AS blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__fraxlend_ez_deposits') }}

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
  market AS protocol_token,
  deposit_asset,
  deposit_amount,
  deposit_amount_usd,
  depositor_address,
  lending_pool_contract,
  issued_deposit_tokens,
  platform,
  symbol,
  blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  deposits
