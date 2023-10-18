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

deposits AS (

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
    aave_version AS platform,
    symbol,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__aave_deposits') }}

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
  spark_market AS deposit_asset,
  spark_token AS market,
  issued_tokens AS deposit_amount,
  (issued_tokens * price) AS supplied_usd,
  depositor_address,
  platform,
  symbol,
  blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__spark_deposits') }}
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
    WHEN supplied_symbol = 'ETH' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    ELSE supplied_contract_addr
  END AS deposit_asset,
  ctoken AS market,
  supplied_base_asset AS deposit_amount,
  supplied_base_asset_usd AS deposit_amount_usd,
  supplier AS depositor_address,
  compound_version AS protocol,
  supplied_symbol AS symbol,
  'ethereum' AS blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__comp_deposits') }}

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
  deposit_asset,
  frax_market_address AS market,
  deposit_amount AS deposit_amount,
  (deposit_amount * price) AS deposit_amount_usd,
  caller AS depositor_address,
  'Fraxlend' AS platform,
  underlying_symbol AS symbol,
  'ethereum' AS blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__fraxlend_deposits') }}
LEFT JOIN 
    prices p
ON 
    deposit_asset = p.token_address
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
  market AS protocol_market,
  deposit_asset,
  deposit_amount,
  deposit_amount_usd,
  depositor_address,
  platform,
  symbol,
  blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  deposits
