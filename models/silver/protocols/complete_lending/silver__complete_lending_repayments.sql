{{ config(
  materialized = 'incremental',
  unique_key = '_log_id',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['non_realtime'],
) }}

WITH repayments AS (

  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    aave_market AS repay_token,
    aave_token AS protocol_token,
    repayed_tokens AS repayed_tokens,
    repayed_usd AS repayed_usd,
    symbol AS repay_symbol,
    payer AS payer_address,
    borrower AS borrower_address,
    lending_pool_contract,
    aave_version AS platform,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__aave_ez_repayments') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
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
  spark_MARKET AS repay_token,
  spark_TOKEN AS protocol_token,
  repayed_tokens AS repayed_tokens,
  repayed_usd AS repayed_usd,
  symbol AS repay_symbol,
  payer AS payer_address,
  borrower AS borrower_address,
  lending_pool_contract,
  platform,
  blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__spark_ez_repayments') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
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
  underlying_asset AS repay_token,
  asset AS protocol_token,
  supply_tokens AS repayed_tokens,
  supply_usd AS repayed_usd,
  compound_market_symbol AS repay_symbol,
  repay_address AS payer_address,
  borrow_address AS borrower_address,
  NULL AS lending_pool_contract,
  compound_version AS platform,
  blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__compv3_ez_repayments') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
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
  repay_contract_address AS repay_token,
  ctoken AS protocol_token,
  repayed_amount AS repay_tokens,
  repayed_amount_usd AS repay_usd,
  repay_contract_symbol AS repay_symbol,
  payer AS payer_address,
  borrower AS borrow_address,
  NULL AS lending_pool_contract,
  'Comp V3' AS platform,
  'ethereum' AS blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('compound__ez_repayments') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
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
  underlying_asset AS repay_token,
  frax_market_address AS protocol_token,
  repay_amount AS repay_tokens,
  repay_amount_usd AS repay_usd,
  frax_market_symbol AS repay_symbol,
  payer AS payer_address,
  borrower AS borrow_address,
  NULL AS lending_pool_contract,
  'Fraxlend' AS platform,
  'ethereum' AS blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__fraxlend_ez_repayments') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
)
SELECT
  *
FROM
  repayments
