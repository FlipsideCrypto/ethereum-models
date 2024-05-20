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
    aave_market AS token_address,
    aave_token AS protocol_token,
    flashloan_amount_unadj,
    flashloan_amount,
    flashloan_amount_usd,
    premium_amount_unadj,
    premium_amount,
    premium_amount_usd,
    initiator_address,
    target_address,
    aave_version AS platform,
    symbol AS token_symbol,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__aave_flashloans') }}

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
    spark_market AS token_address,
    spark_token AS protocol_token,
    flashloan_amount_unadj,
    flashloan_amount,
    NULL AS flashloan_amount_usd,
    premium_amount_unadj,
    premium_amount,
    NULL AS premium_amount_usd,
    initiator_address,
    target_address,
    platform,
    symbol AS token_symbol,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__spark_flashloans') }}

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
    radiant_market AS token_address,
    radiant_token AS protocol_token,
    flashloan_amount_unadj,
    flashloan_amount,
    NULL AS flashloan_amount_usd,
    premium_amount_unadj,
    premium_amount,
    NULL AS premium_amount_usd,
    initiator_address,
    target_address,
    platform,
    symbol AS token_symbol,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__radiant_flashloans') }}

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
    uwu_market AS token_address,
    uwu_token AS protocol_token,
    flashloan_amount_unadj,
    flashloan_amount,
    NULL AS flashloan_amount_usd,
    premium_amount_unadj,
    premium_amount,
    NULL AS premium_amount_usd,
    initiator_address,
    target_address,
    platform,
    symbol AS token_symbol,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__uwu_flashloans') }}

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
flashloan_union AS (
  SELECT
    *
  FROM
    aave
  UNION ALL
  SELECT
    *
  FROM
    radiant
  UNION ALL
  SELECT
    *
  FROM
    spark
  UNION ALL
  SELECT
    *
  FROM
    uwu
),
complete_lending_flashloans AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    'FlashLoan' AS event_name,
    protocol_token AS protocol_market,
    A.token_address AS flashloan_token,
    token_symbol AS flashloan_token_symbol,
    flashloan_amount_unadj,
    flashloan_amount,
    CASE
      WHEN platform <> 'Aave' THEN ROUND((flashloan_amount * price), 2)
      ELSE flashloan_amount_usd
    END AS flashloan_amount_usd,
    premium_amount_unadj,
    premium_amount,
    CASE
      WHEN platform <> 'Aave' THEN ROUND((premium_amount * price), 2)
      ELSE premium_amount_usd
    END AS premium_amount_usd,
    initiator_address AS initiator,
    target_address AS target,
    platform,
    A.blockchain,
    A._LOG_ID,
    A._INSERTED_TIMESTAMP
  FROM
    flashloan_union A
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
    protocol_market,
    flashloan_token,
    flashloan_token_symbol,
    flashloan_amount_unadj,
    flashloan_amount,
    CASE
      WHEN platform <> 'Aave' THEN ROUND((flashloan_amount * price), 2)
      ELSE flashloan_amount_usd
    END AS flashloan_amount_usd_heal,
    premium_amount_unadj,
    premium_amount,
    CASE
      WHEN platform <> 'Aave' THEN ROUND((premium_amount * price), 2)
      ELSE premium_amount_usd
    END AS premium_amount_usd_heal,
    initiator,
    target,
    platform,
    t0.blockchain,
    t0._LOG_ID,
    t0._INSERTED_TIMESTAMP
  FROM
    {{ this }}
    t0
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON t0.flashloan_token = p.token_address
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
        t1.flashloan_amount_usd IS NULL
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
            AND p.token_address = t1.flashloan_token
            AND p.hour = DATE_TRUNC(
              'hour',
              t1.block_timestamp
            )
        )
      GROUP BY
        1
    )
    OR CONCAT(
      t0.block_number,
      '-',
      t0.platform
    ) IN (
      SELECT
        CONCAT(
          t2.block_number,
          '-',
          t2.platform
        )
      FROM
        {{ this }}
        t2
      WHERE
        t2.premium_amount_usd IS NULL
        AND t2._inserted_timestamp < (
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
            AND p.token_address = t2.flashloan_token
            AND p.hour = DATE_TRUNC(
              'hour',
              t2.block_timestamp
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
    complete_lending_flashloans

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
  protocol_market,
  flashloan_token,
  flashloan_token_symbol,
  flashloan_amount_unadj,
  flashloan_amount,
  flashloan_amount_usd_heal AS flashloan_amount_usd,
  premium_amount_unadj,
  premium_amount,
  premium_amount_usd_heal AS premium_amount_usd,
  initiator,
  target,
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
  ) }} AS complete_lending_flashloans_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1
