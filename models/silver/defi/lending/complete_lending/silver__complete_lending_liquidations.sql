{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform'],
  cluster_by = ['block_timestamp::DATE'],
  tags = ['non_realtime','reorg','curation']
) }}

WITH compv2_join AS (

  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    liquidator,
    borrower,
    liquidation_amount,
    liquidation_amount_usd,
    l.ctoken AS protocol_collateral_asset,
    CASE
      WHEN l.ctoken_symbol = 'cETH' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
      ELSE liquidation_contract_address
    END AS collateral_asset,
    liquidation_contract_symbol AS collateral_asset_symbol,
    l.ctoken_symbol AS collateral_symbol,
    collateral_token AS protocol_debt_asset,
    A.underlying_asset_address AS debt_asset,
    A.underlying_symbol AS debt_asset_symbol,
    NULL AS debt_to_cover_amount,
    NULL AS debt_to_cover_amount_usd,
    l.compound_version AS platform,
    'ethereum' AS blockchain,
    l._LOG_ID,
    l._INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__comp_liquidations') }}
    l
    LEFT JOIN {{ ref('silver__comp_asset_details') }} A
    ON l.collateral_token = A.ctoken_address

{% if is_incremental() %}
WHERE
  l._inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
      {{ this }}
  )
{% endif %}
),
liquidation_union AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    liquidator,
    borrower,
    liquidation_amount AS liquidated_amount,
    liquidation_amount_usd AS liquidated_amount_usd,
    protocol_collateral_asset,
    collateral_asset,
    collateral_asset_symbol,
    protocol_debt_asset,
    debt_asset,
    debt_asset_symbol,
    debt_to_cover_amount,
    debt_to_cover_amount_usd,
    platform,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    compv2_join
UNION ALL
SELECT
  tx_hash,
  block_number,
  block_timestamp,
  event_index,
  liquidator,
  borrower,
  liquidated_amount,
  liquidated_amount_usd,
  collateral_aave_token AS protocol_collateral_asset,
  collateral_asset,
  collateral_token_symbol AS collateral_asset_symbol,
  debt_aave_token AS protocol_debt_asset,
  debt_asset,
  debt_token_symbol AS debt_asset_symbol,
  debt_to_cover_amount,
  debt_to_cover_amount_usd,
  aave_version AS platform,
  blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__aave_liquidations') }}

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
  liquidator,
  borrower,
  liquidated_amount,
  liquidated_amount_usd,
  collateral_spark_token AS protocol_collateral_asset,
  collateral_asset,
  collateral_token_symbol AS collateral_asset_symbol,
  debt_spark_token AS protocol_debt_asset,
  debt_asset,
  debt_token_symbol AS debt_asset_symbol,
  debt_to_cover_amount,
  debt_to_cover_amount_usd,
  platform,
  blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__spark_liquidations') }}

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
  liquidator,
  borrower,
  liquidator_repay_amount AS liquidated_amount,
  repay_amount_usd AS liquidated_amount_usd,
  NULL AS protocol_collateral_asset,
  underlying_asset AS collateral_asset,
  C.symbol AS collateral_asset_symbol,
  frax_market_address AS protocol_debt_asset,
  LOWER('0x853d955aCEf822Db058eb8505911ED77F175b99e') AS debt_asset,
  'FRAX' AS debt_asset_symbol,
  NULL AS debt_to_cover_amount,
  NULL AS debt_to_cover_amount_usd,
  'Fraxlend' AS platform,
  'ethereum' AS blockchain,
  f._LOG_ID,
  f._INSERTED_TIMESTAMP
FROM
  {{ ref('silver__fraxlend_liquidations') }}
  f
  LEFT JOIN {{ ref('silver__contracts') }} C
  ON f.underlying_asset = C.address

{% if is_incremental() %}
WHERE
  f._inserted_timestamp >= (
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
  liquidator,
  borrower,
  protocol_collateral_asset,
  collateral_asset,
  collateral_asset_symbol,
  liquidated_amount AS liquidation_amount,
  liquidated_amount_usd AS liquidation_amount_usd,
  protocol_debt_asset,
  debt_asset,
  debt_asset_symbol,
  debt_to_cover_amount,
  debt_to_cover_amount_usd,
  platform,
  blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  liquidation_union
