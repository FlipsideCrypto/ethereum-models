{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform'],
  cluster_by = ['block_timestamp::DATE'],
  tags = ['reorg','curated']
) }}

WITH compv2_join AS (

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
  NULL AS liquidated_amount_usd,
  collateral_spark_token AS protocol_collateral_asset,
  collateral_asset,
  collateral_token_symbol AS collateral_asset_symbol,
  debt_spark_token AS protocol_debt_asset,
  debt_asset,
  debt_token_symbol AS debt_asset_symbol,
  debt_to_cover_amount,
  NULL AS debt_to_cover_amount_usd,
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
  NULL AS liquidated_amount_usd,
  NULL AS protocol_collateral_asset,
  underlying_asset AS collateral_asset,
  underlying_symbol AS collateral_asset_symbol,
  frax_market_address AS protocol_debt_asset,
  debt_asset,
  'FRAX' AS debt_asset_symbol,
  liquidator_repay_amount AS debt_to_cover_amount,
  NULL AS debt_to_cover_amount_usd,
  'Fraxlend' AS platform,
  'ethereum' AS blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__fraxlend_liquidations') }}

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
),
contracts AS (
  SELECT
    *
  FROM
    {{ ref('silver__contracts') }}
  WHERE
    address IN (
      SELECT
        DISTINCT(collateral_asset) AS asset
      FROM
        liquidation_union
      UNION ALL
      SELECT
        DISTINCT(debt_asset) AS asset
      FROM
        liquidation_union
    )
),
prices AS (
  SELECT
    *
  FROM
    {{ ref('core__fact_hourly_token_prices') }}
    p
  WHERE
    token_address IN (
      SELECT
        DISTINCT(collateral_asset) AS asset
      FROM
        liquidation_union
      UNION ALL
      SELECT
        DISTINCT(debt_asset) AS asset
      FROM
        liquidation_union
    )
    AND HOUR > (
      SELECT
        MIN(block_timestamp)
      FROM
        liquidation_union
    )
),
FINAL AS (
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
    CASE
      WHEN platform IN (
        'Fraxlend',
        'Spark'
      ) THEN ROUND((liquidated_amount * p.price), 2)
      ELSE ROUND(
        liquidated_amount_usd,
        2
      )
    END AS liquidation_amount_usd,
    protocol_debt_asset,
    debt_asset,
    debt_asset_symbol,
    debt_to_cover_amount,
    CASE
      WHEN platform IN (
        'Fraxlend',
        'Spark'
      ) THEN ROUND((debt_to_cover_amount * p2.price), 2)
      ELSE ROUND(
        debt_to_cover_amount_usd,
        2
      )
    END AS debt_to_cover_amount_usd,
    platform,
    A.blockchain,
    A._LOG_ID,
    A._INSERTED_TIMESTAMP
  FROM
    liquidation_union A
    LEFT JOIN prices p
    ON collateral_asset = p.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p.hour
    LEFT JOIN contracts C
    ON collateral_asset = C.address
    LEFT JOIN prices p2
    ON debt_asset = p2.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p2.hour
    LEFT JOIN contracts c2
    ON debt_asset = C.address
)
SELECT
  *
FROM
  FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1
