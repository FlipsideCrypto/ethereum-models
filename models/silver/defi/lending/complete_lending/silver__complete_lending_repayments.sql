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
repayments AS (

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
  spark_market AS repay_token,
  spark_token AS protocol_token,
  repayed_tokens AS repay_amount,
  (repayed_tokens * price) AS repay_amount_usd,
  symbol AS repay_symbol,
  payer AS payer_address,
  borrower AS borrower_address,
  platform,
  blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__spark_repayments') }}
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
  (repay_amount * price) AS repay_amount_usd,
  underlying_symbol AS repay_symbol,
  payer AS payer_address,
  borrower AS borrower_address,
  'Fraxlend' AS platform,
  'ethereum' AS blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__fraxlend_repayments') }}

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
