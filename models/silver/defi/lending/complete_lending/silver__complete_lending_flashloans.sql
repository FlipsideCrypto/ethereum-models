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
    flashloan_amount_unadj,
    flashloan_amount,
    flashloan_amount_usd,
    premium_amount_unadj,
    premium_amount,
    premium_amount_usd,
    initiator_address,
    target_address,
    aave_version AS platform,
    symbol as token_symbol,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__aave_flashloans') }}

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
flashloan_union as (
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
    'FlashLoan' AS event_name,
    protocol_token as protocol_market,
    a.token_address as flashloan_token,
    token_symbol as flashloan_token_symbol,
    flashloan_amount_unadj,
    flashloan_amount,
    CASE 
        WHEN platform <> 'Aave'
          THEN ROUND((flashloan_amount * price), 2)
          ELSE flashloan_amount_usd 
    END as flashloan_amount_usd,
    premium_amount_unadj,
    premium_amount,
    CASE 
        WHEN platform <> 'Aave'
          THEN ROUND((premium_amount * price), 2)
          ELSE premium_amount_usd 
    END as premium_amount_usd,
    initiator_address as initiator,
    target_address as target,
    platform,
    a.blockchain,
    a._LOG_ID,
    a._INSERTED_TIMESTAMP
  FROM
    flashloan_union a
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
  ) }} AS complete_lending_flashloans_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1
