-- depends_on: {{ ref('silver__complete_token_prices') }}
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform'],
  cluster_by = ['block_timestamp::DATE'],
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
    depositor_address,
    aave_token AS protocol_market,
    aave_market AS token_address,
    symbol AS token_symbol,
    issued_tokens_unadj AS amount_unadj,
    issued_tokens AS amount,
    supplied_usd AS amount_usd,
    aave_version AS platform,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__aave_deposits') }}

{% if is_incremental() and 'aave' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
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
    depositor_address,
    radiant_token AS protocol_market,
    radiant_market AS token_address,
    symbol AS token_symbol,
    amount_unadj,
    amount,
    NULL AS amount_usd,
    platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__radiant_deposits') }}

{% if is_incremental() and 'radiant' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
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
    depositor_address,
    spark_token AS protocol_market,
    spark_market AS token_address,
    symbol AS token_symbol,
    amount_unadj,
    amount,
    NULL AS amount_usd,
    platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__spark_deposits') }}

{% if is_incremental() and 'spark' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
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
    depositor_address,
    sturdy_token AS protocol_market,
    sturdy_market AS token_address,
    symbol AS token_symbol,
    amount_unadj,
    amount,
    NULL AS amount_usd,
    platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__sturdy_deposits') }}

{% if is_incremental() and 'sturdy' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
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
    depositor_address,
    uwu_token AS protocol_market,
    uwu_market AS token_address,
    symbol AS token_symbol,
    amount_unadj,
    amount,
    NULL AS amount_usd,
    platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__uwu_deposits') }}

{% if is_incremental() and 'uwu' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
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
    supplier AS depositor_address,
    token_address AS protocol_market,
    supplied_contract_addr AS token_address,
    supplied_symbol AS token_symbol,
    amount_unadj,
    amount,
    NULL AS amount_usd,
    platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__cream_deposits') }}

{% if is_incremental() and 'cream' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
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
    supplier AS depositor_address,
    token_address AS protocol_market,
    supplied_contract_addr AS token_address,
    supplied_symbol AS token_symbol,
    amount_unadj,
    amount,
    NULL AS amount_usd,
    platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__flux_deposits') }}

{% if is_incremental() and 'flux' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
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
    supplier AS depositor_address,
    token_address AS protocol_market,
    supplied_contract_addr AS token_address,
    supplied_symbol AS token_symbol,
    amount_unadj,
    amount,
    NULL AS amount_usd,
    platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__strike_deposits') }}

{% if is_incremental() and 'strike' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
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
    supplier AS depositor_address,
    ctoken AS protocol_market,
    CASE
      WHEN supplied_symbol = 'ETH' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
      ELSE supplied_contract_addr
    END AS token_address,
    supplied_symbol AS token_symbol,
    supplied_base_asset_unadj AS amount_unadj,
    supplied_base_asset AS amount,
    supplied_base_asset_usd AS amount_usd,
    compound_version AS platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__comp_deposits') }}

{% if is_incremental() and 'comp' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
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
    caller AS depositor_address,
    frax_market_address AS protocol_market,
    deposit_asset AS token_address,
    underlying_symbol AS token_symbol,
    deposit_amount_unadj AS amount_unadj,
    deposit_amount AS amount,
    NULL AS amount_usd,
    'Fraxlend' AS platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__fraxlend_deposits') }}

{% if is_incremental() and 'fraxlend' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(
        _inserted_timestamp
      ) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
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
    depositor_address,
    silo_market AS protocol_market,
    token_address,
    token_symbol,
    amount_unadj,
    amount,
    NULL AS amount_usd,
    platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__silo_deposits') }}

{% if is_incremental() and 'silo' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
    FROM
      {{ this }}
  )
{% endif %}
),
deposit_union AS (
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
complete_lending_deposits AS (
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
      WHEN platform IN (
        'Compound V2',
        'Cream',
        'Flux',
        'Strike'
      ) THEN 'Mint'
      WHEN platform IN (
        'Spark',
        'Aave V3',
        'Radiant',
        'Sturdy',
        'UwU'
      ) THEN 'Supply'
      ELSE 'Deposit'
    END AS event_name,
    depositor_address AS depositor,
    protocol_market,
    A.token_address,
    A.token_symbol,
    amount_unadj,
    amount,
    CASE
      WHEN platform NOT IN (
        'Aave',
        'Compound'
      ) THEN ROUND((amount * price), 2)
      ELSE amount_usd
    END AS amount_usd,
    platform,
    A.blockchain,
    A._LOG_ID,
    A._INSERTED_TIMESTAMP
  FROM
    deposit_union A
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON A.token_address = p.token_address
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
    depositor,
    protocol_market,
    t0.token_address,
    t0.token_symbol,
    amount_unadj,
    amount,
    CASE
      WHEN platform NOT IN (
        'Aave',
        'Compound'
      ) THEN ROUND((amount * price), 2)
      ELSE amount_usd
    END AS amount_usd,
    platform,
    t0.blockchain,
    t0._LOG_ID,
    t0._INSERTED_TIMESTAMP
  FROM
    {{ this }}
    t0
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON t0.token_address = p.token_address
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
            ) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
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
            AND p.token_address = t1.token_address
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
    complete_lending_deposits

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
UNION ALL
SELECT
  *
FROM
  heal_model
{% endif %}
)
SELECT
  *,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_hash','event_index']
  ) }} AS complete_lending_deposits_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1
