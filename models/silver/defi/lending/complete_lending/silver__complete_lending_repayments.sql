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
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
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
  origin_from_address,
  origin_to_address,
  origin_function_signature,
  contract_address,
  spark_market AS repay_token,
  spark_token AS protocol_token,
  repayed_tokens AS repay_amount,
  NULL AS repay_amount_usd,
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
  compound_version AS platform,
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
  origin_from_address,
  origin_to_address,
  origin_function_signature,
  contract_address,
  repay_asset AS repay_token,
  frax_market_address AS protocol_token,
  repay_amount AS repay_amount,
  NULL AS repay_amount_usd,
  repay_symbol,
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
  origin_from_address,
  origin_to_address,
  origin_function_signature,
  contract_address,
  CASE 
    WHEN platform = 'Fraxlend' THEN 'RepayAsset'
    WHEN platform = 'Compound V3' THEN 'Supply'
    WHEN platform = 'Compound V2' THEN 'RepayBorrow'
    ELSE 'Repay'
  END AS event_name,
  protocol_token as protocol_market,
  repay_token as repay_asset,
  repay_amount,
  CASE
        WHEN platform IN ('Fraxlenmd','Spark') 
        THEN ROUND(repay_amount * price / pow(10,C.decimals),2)
        ELSE ROUND(repay_amount_usd,2) 
  END AS repay_amount_usd,
  repay_symbol,
  payer_address,
  borrower_address,
  platform,
  blockchain,
  a._LOG_ID,
  a._INSERTED_TIMESTAMP
FROM
  repayments a
LEFT JOIN {{ ref('core__fact_hourly_token_prices') }} p
ON repay_token = p.token_address
AND DATE_TRUNC(
    'hour',
    block_timestamp
) = p.hour
LEFT JOIN {{ ref('silver__contracts') }} C
ON repay_token = C.address
