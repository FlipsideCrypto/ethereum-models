{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform'],
  cluster_by = ['block_timestamp::DATE'],
  tags = ['non_realtime','reorg','curation']
) }}

WITH prices AS (

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
      UNION
      SELECT
        '0x853d955acef822db058eb8505911ed77f175b99e' AS underlying_asset
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
compv2_join AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    liquidator,
    borrower,
    liquidation_amount,
    liquidation_amount_usd,
    l.ctoken AS protocol_collateral_asset,
    liquidation_contract_address AS collateral_asset,
    liquidation_contract_symbol AS collateral_asset_symbol,
    l.ctoken_symbol AS collateral_symbol,
    collateral_ctoken AS protocol_debt_asset,
    collateral_token AS debt_asset,
    collateral_symbol AS debt_asset_symbol,
    NULL AS debt_to_cover_amount,
    NULL AS debt_to_cover_amount_usd,
    l.compound_version AS platform,
    'ethereum' AS blockchain,
    l._LOG_ID,
    l._INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__comp_liquidations') }}
    l

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
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
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
        origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
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
      origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
  liquidator,
  borrower,
  liquidated_amount,
  (
    liquidated_amount * price
  ) AS liquidated_amount_usd,
  collateral_spark_token AS protocol_collateral_asset,
  collateral_asset,
  collateral_token_symbol AS collateral_asset_symbol,
  debt_spark_token AS protocol_debt_asset,
  debt_asset,
  debt_token_symbol AS debt_asset_symbol,
  NULL AS debt_to_cover_amount,
  NULL AS debt_to_cover_amount_usd,
  platform,
  blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__spark_liquidations') }}
  LEFT JOIN prices p
  ON collateral_spark_token = p.token_address
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
  origin_from_address,
  origin_to_address,
  origin_function_signature,
  contract_address,
  liquidator,
  borrower,
  collateral_for_liquidator AS liquidated_amount,
  (
    collateral_for_liquidator * p.price
  ) AS liquidated_amount_usd,
  NULL AS protocol_collateral_asset,
  underlying_asset AS collateral_asset,
  underlying_symbol AS collateral_asset_symbol,
  frax_market_address AS protocol_debt_asset,
  debt_asset,
  'FRAX' AS debt_asset_symbol,
  liquidator_repay_amount AS debt_to_cover_amount,
  (
    liquidator_repay_amount * p2.price
  ) AS debt_to_cover_amount_usd,
  'Fraxlend' AS platform,
  'ethereum' AS blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__fraxlend_liquidations') }}
  LEFT JOIN prices p
  ON underlying_asset = p.token_address
  AND DATE_TRUNC(
    'hour',
    block_timestamp
  ) = p.hour
  LEFT JOIN prices p2
  ON debt_asset = p2.token_address
  AND DATE_TRUNC(
    'hour',
    block_timestamp
  ) = p2.hour

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
    WHEN platform = 'Fraxlend' THEN 'Liquidate'
    WHEN platform = 'Compound V3' THEN 'AbsorbCollateral'
    WHEN platform = 'Compound V2' THEN 'LiquidateBorrow'
    ELSE 'LiquidationCall'
  END AS event_name,
  liquidator,
  borrower,
  protocol_collateral_asset,
  collateral_asset,
  collateral_asset_symbol,
  liquidated_amount AS liquidation_amount,
  ROUND(liquidated_amount_usd,2) AS liquidation_amount_usd,
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
