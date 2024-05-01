{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform'],
  cluster_by = ['block_timestamp::DATE'],
  tags = ['reorg','curated']
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
    aave_market AS token_address,
    aave_token AS protocol_token,
    repayed_tokens_unadj AS amount_unadj,
    repayed_tokens AS amount,
    repayed_usd AS amount_usd,
    symbol AS token_symbol,
    payer AS payer_address,
    borrower,
    aave_version AS platform,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__aave_repayments') }}

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
    radiant_market AS token_address,
    radiant_token AS protocol_market,
    amount_unadj,
    amount,
    NULL AS amount_usd,
    symbol AS token_symbol,
    payer AS payer_address,
    borrower,
    platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__radiant_repayments') }}

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
    spark_market AS token_address,
    spark_token AS protocol_market,
    amount_unadj,
    amount,
    NULL AS amount_usd,
    symbol AS token_symbol,
    payer AS payer_address,
    borrower,
    platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__spark_repayments') }}

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
    sturdy_market AS token_address,
    sturdy_token AS protocol_market,
    amount_unadj,
    amount,
    NULL AS amount_usd,
    symbol AS token_symbol,
    payer AS payer_address,
    borrower,
    platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__sturdy_repayments') }}

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
    uwu_market AS token_address,
    uwu_token AS protocol_market,
    amount_unadj,
    amount,
    NULL AS amount_usd,
    symbol AS token_symbol,
    payer AS payer_address,
    borrower,
    platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__uwu_repayments') }}

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
    repay_contract_address AS token_address,
    token_address AS protocol_market,
    amount_unadj,
    amount,
    NULL AS amount_usd,
    repay_contract_symbol AS token_symbol,
    payer AS payer_address,
    borrower,
    platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__cream_repayments') }}

 {% if is_incremental() and 'cream' not in var('HEAL_CURATED_MODEL') %}
  WHERE
    _inserted_timestamp >= (
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
    repay_contract_address AS token_address,
    token_address AS protocol_market,
    amount_unadj,
    amount,
    NULL AS amount_usd,
    repay_contract_symbol AS token_symbol,
    payer AS payer_address,
    borrower,
    platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__flux_repayments') }}

 {% if is_incremental() and 'flux' not in var('HEAL_CURATED_MODEL') %}
  WHERE
    _inserted_timestamp >= (
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
    repay_contract_address AS token_address,
    token_address AS protocol_market,
    amount_unadj,
    amount,
    NULL AS amount_usd,
    repay_contract_symbol AS token_symbol,
    payer AS payer_address,
    borrower,
    platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__strike_repayments') }}

 {% if is_incremental() and 'strike' not in var('HEAL_CURATED_MODEL') %}
  WHERE
    _inserted_timestamp >= (
      SELECT
        MAX(_inserted_timestamp) - INTERVAL '36 hours'
      FROM
        {{ this }}
    )
  {% endif %}
),
comp as (
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
    END AS token_address,
    ctoken AS protocol_market,
    repayed_amount_unadj AS amount_unadj,
    repayed_amount AS amount,
    repayed_amount_usd AS amount_usd,
    repay_contract_symbol AS token_symbol,
    payer AS payer_address,
    borrower AS borrower,
    compound_version AS platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__comp_repayments') }}

  {% if is_incremental() and 'comp' not in var('HEAL_CURATED_MODEL') %}
  WHERE
    _inserted_timestamp >= (
      SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
      FROM
        {{ this }}
    )
  {% endif %}
),
fraxlend as (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    repay_asset as tokem_address,
    frax_market_address AS protocol_token,
    repay_amount_unadj AS amount_unadj,
    repay_amount AS amount,
    NULL AS amount_usd,
    repay_symbol token_symbol,
    payer AS payer_address,
    borrower AS borrower,
    'Fraxlend' AS platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__fraxlend_repayments') }}

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
    token_address,
    silo_market AS protocol_market,
    amount_unadj,
    amount,
    NULL AS amount_usd,
    token_symbol,
    NULL AS payer_address,
    depositor_address AS borrower,
    platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__silo_repayments') }}

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
repay_union as (
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
      WHEN platform = 'Fraxlend' THEN 'RepayAsset'
      WHEN platform = 'Compound V3' THEN 'Supply'
      WHEN platform IN (
        'Compound V2',
        'Cream',
        'Flux',
        'Strike') THEN 'RepayBorrow'
      ELSE 'Repay'
    END AS event_name,
    protocol_token AS protocol_market,
    a.token_address,
    amount_unadj,
    amount,
    CASE
      WHEN platform NOT LIKE '%Aave%' OR platform NOT LIKE '%Compound%'
      THEN ROUND((amount * price), 2)
      ELSE ROUND(
        amount_usd,
        2
      )
    END AS amount_usd,
    token_symbol,
    payer_address as payer,
    borrower,
    platform,
    A.blockchain,
    A._LOG_ID,
    A._INSERTED_TIMESTAMP
  FROM
    repay_union A
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON a.token_address = p.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p.hour
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON a.token_address = C.address
)
SELECT
  *,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_hash','event_index']
  ) }} AS complete_lending_repayments_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1
