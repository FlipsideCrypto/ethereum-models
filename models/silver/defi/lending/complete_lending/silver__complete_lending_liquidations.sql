-- depends_on: {{ ref('silver__complete_token_prices') }}
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform'],
  cluster_by = ['block_timestamp::DATE','platform'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, origin_from_address, origin_to_address, origin_function_signature, contract_address, event_name, liquidator, borrower, collateral_token, collateral_token_symbol, debt_token, debt_token_symbol, protocol_market), SUBSTRING(origin_function_signature, event_name, liquidator, borrower, collateral_token, collateral_token_symbol, debt_token, debt_token_symbol, protocol_market)",
  tags = ['reorg','curated','heal']
) }}

WITH aave AS (

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

{% if is_incremental() and 'aave' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
radiant AS (
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

{% if is_incremental() and 'radiant' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
spark AS (
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

{% if is_incremental() and 'spark' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
sturdy AS (
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

{% if is_incremental() and 'sturdy' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
uwu AS (
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

{% if is_incremental() and 'uwu' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
cream AS (
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

{% if is_incremental() and 'cream' not in var('HEAL_MODELS') %}
WHERE
  l._inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
flux AS (
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

{% if is_incremental() and 'flux' not in var('HEAL_MODELS') %}
WHERE
  l._inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
strike AS (
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

{% if is_incremental() and 'strike' not in var('HEAL_MODELS') %}
WHERE
  l._inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
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

{% if is_incremental() and 'comp' not in var('HEAL_MODELS') %}
WHERE
  l._inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
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

{% if is_incremental() and 'fraxlend' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(
        _inserted_timestamp
      ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
silo AS (
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

{% if is_incremental() and 'silo' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
liquidation_union AS (
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
complete_lending_liquidations AS (
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
        'Strike'
      ) THEN 'LiquidateBorrow'
      ELSE 'LiquidationCall'
    END AS event_name,
    liquidator,
    borrower,
    protocol_collateral_asset AS protocol_market,
    collateral_asset AS collateral_token,
    collateral_asset_symbol AS collateral_token_symbol,
    liquidated_amount_unadj AS amount_unadj,
    liquidated_amount AS amount,
    CASE
      WHEN platform NOT LIKE '%Aave%'
      OR platform NOT LIKE '%Compound%' THEN ROUND((liquidated_amount * p.price), 2)
      ELSE ROUND(
        liquidated_amount_usd,
        2
      )
    END AS amount_usd,
    debt_asset AS debt_token,
    debt_asset_symbol AS debt_token_symbol,
    platform,
    A.blockchain,
    A._LOG_ID,
    A._INSERTED_TIMESTAMP
  FROM
    liquidation_union A
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON A.collateral_asset = p.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p.hour
),

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
heal_model AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    event_name,
    liquidator,
    borrower,
    protocol_market,
    collateral_token,
    collateral_token_symbol,
    amount_unadj,
    amount,
    CASE
      WHEN platform NOT LIKE '%Aave%'
      OR platform NOT LIKE '%Compound%' THEN ROUND((amount * p.price), 2)
      ELSE ROUND(
        amount_usd,
        2
      )
    END AS amount_usd_heal,
    debt_token,
    debt_token_symbol,
    platform,
    t0.blockchain,
    t0._LOG_ID,
    t0._INSERTED_TIMESTAMP
  FROM
    {{ this }}
    t0
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON t0.collateral_token = p.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p.hour
  WHERE
    CONCAT(
      t0.block_number,
      '-',
      t0.platform
    ) IN (
      SELECT
        CONCAT(
          t1.block_number,
          '-',
          t1.platform
        )
      FROM
        {{ this }}
        t1
      WHERE
        t1.amount_usd IS NULL
        AND t1._inserted_timestamp < (
          SELECT
            MAX(
              _inserted_timestamp
            ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
          FROM
            {{ this }}
        )
        AND EXISTS (
          SELECT
            1
          FROM
            {{ ref('silver__complete_token_prices') }}
            p
          WHERE
            p._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
            AND p.price IS NOT NULL
            AND p.token_address = t1.collateral_token
            AND p.hour = DATE_TRUNC(
              'hour',
              t1.block_timestamp
            )
        )
      GROUP BY
        1
    )
),
{% endif %}

FINAL AS (
  SELECT
    *
  FROM
    complete_lending_liquidations

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
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
  event_name,
  liquidator,
  borrower,
  protocol_market,
  collateral_token,
  collateral_token_symbol,
  amount_unadj,
  amount,
  amount_usd_heal AS amount_usd,
  debt_token,
  debt_token_symbol,
  platform,
  blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  heal_model
{% endif %}
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
