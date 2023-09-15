{{ config(
  materialized = 'incremental',
  unique_key = "_log_id",
  cluster_by = ['block_timestamp::DATE'],
  tags = ['non_realtime']
) }}

WITH compv2_join AS (

  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    liquidation_contract_address AS collateral_asset,
    liquidator,
    borrower,
    liquidation_amount,
    liquidation_amount_usd,
    l.ctoken AS protcol_collateral_asset,
    l.ctoken_symbol AS protcol_collateral_symbol,
    collateral_ctoken AS protocol_debt_asset,
    A.underlying_asset_address AS debt_asset,
    A.underlying_symbol AS debt_asset_symbol,
    NULL AS debt_to_cover_amount,
    NULL AS debt_to_cover_amount_usd,
    'Comp V2' AS platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('compound__ez_liquidations') }}
    l
    LEFT JOIN {{ ref('compound__ez_asset_details') }} A
    ON l.collateral_ctoken = A.ctoken_address

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
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
    collateral_asset,
    liquidator,
    borrower,
    liquidation_amount,
    liquidation_amount_usd,
    protcol_collateral_asset,
    protcol_collateral_symbol,
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
    asset AS collateral_asset,
    absorber AS liquidator,
    borrower,
    tokens_seized AS liquidation_amount,
    liquidation_amount AS liquidation_amount_usd,
    NULL AS protocol_collateral_asset,
    NULL AS protocol_collateral_symbol,
    compound_market AS protocol_debt_asset,
    debt_asset,
    debt_token_symbol AS debt_asset_symbol,
    NULL AS debt_to_cover_amount,
    NULL AS debt_to_cover_amount_usd,
    compound_version AS platform,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__compv3_ez_liquidations') }}

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
  collateral_asset,
  liquidator,
  borrower,
  liquidated_amount,
  liquidated_amount_usd,
  collateral_aave_token AS protocol_collateral_token,
  collateral_token_symbol AS protocol_collateral_symbol,
  debt_aave_token AS protocol_debt_asset,
  debt_asset,
  debt_token_symbol,
  debt_to_cover_amount,
  debt_to_cover_amount_usd,
  aave_version AS platform,
  blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__aave_ez_liquidations') }}

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
  collateral_asset,
  liquidator,
  borrower,
  liquidated_amount,
  liquidated_amount_usd,
  collateral_spark_token AS protocol_collateral_token,
  collateral_token_symbol AS protocol_collateral_symbol,
  debt_aave_token AS protocol_debt_asset,
  debt_asset,
  debt_token_symbol,
  debt_to_cover_amount,
  debt_to_cover_amount_usd,
  platform,
  blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__spark_ez_liquidations') }}

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
  underlying_asset as collateral_asset,
  liquidator,
  borrower,
  liquidator_repay_amount AS liquidated_amount,
  repay_amount_usd AS liquidated_amount_usd,
  NULL AS protocol_collateral_token,
  NULL AS protocol_collateral_symbol,
  frax_market_address AS protocol_debt_asset,
  LOWER('0x853d955aCEf822Db058eb8505911ED77F175b99e') AS debt_asset,
  'FRAX' AS debt_asset_symbol,
  NULL as debt_to_cover_amount,
  NULL as debt_to_cover_amount_usd,
  'Fraxlend' AS platform,
  'ethereum' AS blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__fraxlend_ez_liquidations') }}

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
  liquidation_union

