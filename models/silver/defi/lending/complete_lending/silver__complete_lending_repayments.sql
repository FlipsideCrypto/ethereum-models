{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform'],
  cluster_by = ['block_timestamp::DATE'],
  tags = ['non_realtime','reorg','curation']
) }}

WITH repayments AS (

  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    aave_market AS repay_token,
    aave_token AS protocol_token,
    repayed_tokens AS repay_amount,
    repayed_usd AS repay_amount_usd,
    symbol AS repay_symbol,
    payer AS payer_address,
    borrower AS borrower_address,
    aave_version AS platform,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__aave_repayments') }}

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
  spark_MARKET AS repay_token,
  spark_TOKEN AS protocol_token,
  repayed_tokens AS repay_amount,
  repayed_usd AS repay_amount_usd,
  symbol AS repay_symbol,
  payer AS payer_address,
  borrower AS borrower_address,
  platform,
  blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__spark_repayments') }}

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
  CASE
    WHEN repay_contract_symbol = 'ETH' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    ELSE repay_contract_address
  END AS repay_token,
  ctoken AS protocol_token,
  repayed_amount AS repay_amount,
  repayed_amount_usd AS repay_amount_usd,
  repay_contract_symbol AS repay_symbol,
  payer AS payer_address,
  borrower AS borrower_address,
  compound_version AS protocol,
  'ethereum' AS blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__comp_repayments') }}

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
  underlying_asset AS repay_token,
  frax_market_address AS protocol_token,
  repay_amount AS repay_amount,
  repay_amount_usd AS repay_amount_usd,
  frax_market_symbol AS repay_symbol,
  payer AS payer_address,
  borrower AS borrower_address,
  'Fraxlend' AS platform,
  'ethereum' AS blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__fraxlend_repayments') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '12 hours'
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
  protocol_token as protocol_market,
  repay_token,
  repay_amount,
  repay_amount_usd,
  repay_symbol,
  payer_address,
  borrower_address,
  platform,
  blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  repayments
