{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform'],
  cluster_by = ['block_timestamp::DATE'],
  tags = ['non_realtime','reorg','curation']
) }}

WITH deposits AS (

  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
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
  origin_from_address,
  origin_to_address,
  origin_function_signature,
  contract_address,
  spark_market AS deposit_asset,
  spark_token AS market,
  issued_tokens AS deposit_amount,
  NULL AS deposit_amount_usd,
  depositor_address,
  platform,
  symbol,
  blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__spark_deposits') }}
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
    WHEN supplied_symbol = 'ETH' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    ELSE supplied_contract_addr
  END AS deposit_asset,
  ctoken AS market,
  supplied_base_asset AS deposit_amount,
  supplied_base_asset_usd AS deposit_amount_usd,
  supplier AS depositor_address,
  compound_version AS platform,
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
  origin_from_address,
  origin_to_address,
  origin_function_signature,
  contract_address,
  deposit_asset,
  frax_market_address AS market,
  deposit_amount AS deposit_amount,
  NULL AS deposit_amount_usd,
  caller AS depositor_address,
  'Fraxlend' AS platform,
  underlying_symbol AS symbol,
  'ethereum' AS blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__fraxlend_deposits') }}

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
    WHEN platform = 'Fraxlend' THEN 'AddCollateral'
    WHEN platform = 'Compound V3' THEN 'SupplyCollateral'
    WHEN platform = 'Compound V2' THEN 'Mint'
    WHEN platform in ('Spark','Aave V3') THEN 'Supply'
    ELSE 'Deposit'
  END AS event_name,
  market AS protocol_market,
  deposit_asset,
  deposit_amount,
  CASE
    WHEN platform IN ('Fraxlenmd','Spark') 
    THEN ROUND(deposit_amount * price / pow(10,C.decimals),2)
    ELSE ROUND(deposit_amount_usd,2) 
    END AS deposit_amount_usd,
  depositor_address,
  platform,
  a.symbol,
  a.blockchain,
  a._LOG_ID,
  a._INSERTED_TIMESTAMP
FROM
  deposits a
LEFT JOIN {{ ref('core__fact_hourly_token_prices') }} p
ON deposit_asset = p.token_address
AND DATE_TRUNC(
    'hour',
    block_timestamp
) = p.hour
LEFT JOIN {{ ref('silver__contracts') }} C
ON deposit_asset = C.addressw