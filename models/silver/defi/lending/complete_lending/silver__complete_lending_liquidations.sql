{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform'],
  cluster_by = ['block_timestamp::DATE'],
  tags = ['reorg','curated']
) }}

WITH aave as (
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
    liquidated_amount_unadj,
    liquidated_amount,
    liquidated_amount_usd,
    collateral_aave_token AS protocol_collateral_asset,
    collateral_asset,
    collateral_token_symbol AS collateral_asset_symbol,
    debt_asset,
    debt_token_symbol AS debt_asset_symbol,
    aave_version AS platform,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__aave_liquidations') }}

{% if is_incremental() and 'aave' not in var('HEAL_CURATED_MODEL') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
      {{ this }}
  )
{% endif %}
),

radiant as (
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
    amount_unadj,
    amount AS liquidated_amount,
    NULL AS liquidated_amount_usd,
    collateral_radiant_token AS protocol_collateral_asset,
    collateral_asset,
    collateral_token_symbol AS collateral_asset_symbol,
    debt_asset,
    debt_token_symbol AS debt_asset_symbol,
    'Radiant' AS platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__radiant_liquidations') }}

  {% if is_incremental() and 'radiant' not in var('HEAL_CURATED_MODEL') %}
  WHERE
    _inserted_timestamp >= (
      SELECT
        MAX(_inserted_timestamp) - INTERVAL '36 hours'
      FROM
        {{ this }}
    )
  {% endif %}
),
spark as (
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
    amount_unadj,
    amount AS liquidated_amount,
    NULL AS liquidated_amount_usd,
    collateral_spark_token AS protocol_collateral_asset,
    collateral_asset,
    collateral_token_symbol AS collateral_asset_symbol,
    debt_asset,
    debt_token_symbol AS debt_asset_symbol,
    'Spark' AS platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__spark_liquidations') }}

  {% if is_incremental() and 'spark' not in var('HEAL_CURATED_MODEL') %}
  WHERE
    _inserted_timestamp >= (
      SELECT
        MAX(_inserted_timestamp) - INTERVAL '36 hours'
      FROM
        {{ this }}
    )
  {% endif %}
),
sturdy as (
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
    amount_unadj,
    amount AS liquidated_amount,
    NULL AS liquidated_amount_usd,
    collateral_sturdy_token AS protocol_collateral_asset,
    collateral_asset,
    collateral_token_symbol AS collateral_asset_symbol,
    debt_asset,
    debt_token_symbol AS debt_asset_symbol,
    'Sturdy' AS platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__sturdy_liquidations') }}

  {% if is_incremental() and 'sturdy' not in var('HEAL_CURATED_MODEL') %}
  WHERE
    _inserted_timestamp >= (
      SELECT
        MAX(_inserted_timestamp) - INTERVAL '36 hours'
      FROM
        {{ this }}
    )
  {% endif %}
),
uwu as (
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
    amount_unadj,
    amount AS liquidated_amount,
    NULL AS liquidated_amount_usd,
    collateral_uwu_token AS protocol_collateral_asset,
    collateral_asset,
    collateral_token_symbol AS collateral_asset_symbol,
    debt_asset,
    debt_token_symbol AS debt_asset_symbol,
    'UwU' AS platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__uwu_liquidations') }}

  {% if is_incremental() and 'uwu' not in var('HEAL_CURATED_MODEL') %}
  WHERE
    _inserted_timestamp >= (
      SELECT
        MAX(_inserted_timestamp) - INTERVAL '36 hours'
      FROM
        {{ this }}
    )
  {% endif %}
),
cream as (
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
    amount_unadj,
    amount AS liquidated_amount,
    NULL AS liquidated_amount_usd,
    token AS protocol_collateral_asset,
    liquidation_contract_address AS debt_asset,
    liquidation_contract_symbol AS debt_asset_symbol,
    collateral_token AS collateral_asset,
    collateral_symbol AS collateral_asset_symbol,
    platform,
    'ethereum' AS blockchain,
    l._LOG_ID,
    l._INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__cream_liquidations') }}
    l

{% if is_incremental() and 'cream' not in var('HEAL_CURATED_MODEL') %}
WHERE
  l._inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
),
flux as (
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
    amount_unadj,
    amount AS liquidated_amount,
    NULL AS liquidated_amount_usd,
    token AS protocol_collateral_asset,
    liquidation_contract_address AS debt_asset,
    liquidation_contract_symbol AS debt_asset_symbol,
    collateral_token AS collateral_asset,
    collateral_symbol AS collateral_asset_symbol,
    platform,
    'ethereum' AS blockchain,
    l._LOG_ID,
    l._INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__flux_liquidations') }}
    l

{% if is_incremental() and 'flux' not in var('HEAL_CURATED_MODEL') %}
WHERE
  l._inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
),
strike as (
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
    amount_unadj,
    amount AS liquidated_amount,
    NULL AS liquidated_amount_usd,
    token AS protocol_collateral_asset,
    liquidation_contract_address AS debt_asset,
    liquidation_contract_symbol AS debt_asset_symbol,
    collateral_token AS collateral_asset,
    collateral_symbol AS collateral_asset_symbol,
    platform,
    'ethereum' AS blockchain,
    l._LOG_ID,
    l._INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__strike_liquidations') }}
    l

{% if is_incremental() and 'strike' not in var('HEAL_CURATED_MODEL') %}
WHERE
  l._inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
),

comp AS (

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
    liquidation_amount_unadj AS amount_unadj,
    liquidation_amount AS amount,
    liquidation_amount_usd,
    l.ctoken AS protocol_collateral_asset,
    liquidation_contract_address AS debt_asset,
    liquidation_contract_symbol AS debt_asset_symbol,
    collateral_token AS collateral_asset,
    collateral_symbol AS collateral_asset_symbol,
    l.compound_version AS platform,
    'ethereum' AS blockchain,
    l._LOG_ID,
    l._INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__comp_liquidations') }}
    l

{% if is_incremental() and 'comp' not in var('HEAL_CURATED_MODEL') %}
WHERE
  l._inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
      {{ this }}
  )
{% endif %}
),
fraxlend AS (
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
    collateral_for_liquidator_unadj AS amount_unadj,
    collateral_for_liquidator AS amount,
    NULL AS liquidated_amount_usd,
    NULL AS protocol_collateral_asset,
    underlying_asset AS collateral_asset,
    underlying_symbol AS collateral_asset_symbol,
    debt_asset,
    'FRAX' AS debt_asset_symbol,
    'Fraxlend' AS platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__fraxlend_liquidations') }}

  {% if is_incremental() and 'fraxlend' not in var('HEAL_CURATED_MODEL') %}
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
silo as (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    receiver_address AS liquidator,
    depositor_address AS borrower,
    amount_unadj,
    amount AS liquidated_amount,
    NULL AS liquidated_amount_usd,
    protocol_collateral_token AS protocol_collateral_asset,
    token_address AS collateral_asset,
    token_symbol AS collateral_asset_symbol,
    debt_asset,
    debt_asset_symbol,
    platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__silo_liquidations') }}

  {% if is_incremental() and 'silo' not in var('HEAL_CURATED_MODEL') %}
  WHERE
    _inserted_timestamp >= (
      SELECT
        MAX(_inserted_timestamp) - INTERVAL '36 hours'
      FROM
        {{ this }}
    )
  {% endif %}
),
liquidation_union as (
    SELECT
        *
    FROM
        aave
    UNION ALL
    SELECT
        *
    FROM
        comp
    UNION ALL
    SELECT
        *
    FROM
        cream
    UNION ALL
    SELECT
        *
    FROM
        flux
    UNION ALL
    SELECT
        *
    FROM
        fraxlend
    UNION ALL
    SELECT
        *
    FROM
        radiant
    UNION ALL
    SELECT
        *
    FROM
        silo
    UNION ALL
    SELECT
        *
    FROM
        spark
    UNION ALL
    SELECT
        *
    FROM
        strike
    UNION ALL
    SELECT
        *
    FROM
        sturdy
    UNION ALL
    SELECT
        *
    FROM
        uwu
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
    {{ ref('price__ez_hourly_token_prices') }}
    p
  WHERE
    token_address IN (
      SELECT
        DISTINCT(collateral_asset) AS asset
      FROM
        liquidation_union)
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
      WHEN platform IN (
        'Compound V2',
        'Cream',
        'Flux',
        'Strike') THEN 'LiquidateBorrow'
      ELSE 'LiquidationCall'
    END AS event_name,
    liquidator,
    borrower,
    protocol_collateral_asset as protocol_market,
    collateral_asset as collateral_token,
    collateral_asset_symbol as collateral_token_symbol,
    liquidated_amount_unadj AS amount_unadj,
    liquidated_amount AS amount,
    CASE
      WHEN platform NOT LIKE '%Aave%' OR platform NOT LIKE '%Compound%'
      THEN ROUND((liquidated_amount * p.price), 2)
      ELSE ROUND(
        liquidated_amount_usd,
        2
      )
    END AS amount_usd,
    debt_asset as debt_token,
    debt_asset_symbol as debt_token_symbol,
    platform,
    A.blockchain,
    A._LOG_ID,
    A._INSERTED_TIMESTAMP
  FROM
    liquidation_union A
    LEFT JOIN prices p
    ON a.collateral_asset = p.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p.hour
    LEFT JOIN contracts C
    ON a.collateral_asset = C.address
    LEFT JOIN contracts c2
    ON a.debt_asset = C.address
)
SELECT
  *,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_hash','event_index']
  ) }} AS complete_lending_liquidations_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1
