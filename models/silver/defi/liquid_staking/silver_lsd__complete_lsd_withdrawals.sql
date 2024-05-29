-- depends_on: {{ ref('silver__complete_token_prices') }}
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform','version'],
  cluster_by = ['block_timestamp::DATE'],
  tags = ['curated','reorg','heal']
) }}

WITH ankr AS (

  SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount AS eth_amount_unadjusted,
    eth_amount_adj AS eth_amount_adjusted,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__ankr_withdrawals') }}

{% if is_incremental() and 'ankr' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
coinbase AS (
  SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    sender,
    recipient,
    NULL AS eth_amount_unadjusted,
    NULL AS eth_amount_adjusted,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__coinbase_withdrawals') }}

{% if is_incremental() and 'coinbase' not in var('HEAL_MODELS') %}
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
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount AS eth_amount_unadjusted,
    eth_amount_adj AS eth_amount_adjusted,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__cream_withdrawals') }}

{% if is_incremental() and 'cream' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
frax AS (
  SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount AS eth_amount_unadjusted,
    eth_amount_adj AS eth_amount_adjusted,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__fraxether_withdrawals') }}

{% if is_incremental() and 'frax' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
lido AS (
  SELECT
    C.block_number,
    C.block_timestamp,
    C.origin_function_signature,
    C.origin_from_address,
    C.origin_to_address,
    C.tx_hash,
    C.event_index,
    C.event_name,
    C.contract_address,
    C.sender,
    C.recipient,
    C.eth_amount,
    C.eth_amount_adj,
    amount_of_shares AS token_amount,
    amount_of_shares_adj AS token_amount_adj,
    C.token_address,
    C.token_symbol,
    C.platform,
    'v1' AS version,
    C._log_id,
    C._inserted_timestamp
  FROM
    {{ ref('silver_lsd__lido_withdrawals_claimed') }} C
    LEFT JOIN {{ ref('silver_lsd__lido_withdrawal_requests') }}
    r USING(request_id)

{% if is_incremental() and 'lido' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
nodedao AS (
  SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount AS eth_amount_unadjusted,
    eth_amount_adj AS eth_amount_adjusted,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__nodedao_withdrawals') }}

{% if is_incremental() and 'nodedao' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
rocketpool AS (
  SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount AS eth_amount_unadjusted,
    eth_amount_adj AS eth_amount_adjusted,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__rocketpool_withdrawals') }}

{% if is_incremental() and 'rocketpool' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
sharedstake AS (
  SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount AS eth_amount_unadjusted,
    eth_amount_adj AS eth_amount_adjusted,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__sharedstake_withdrawals') }}

{% if is_incremental() and 'sharedstake' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
sharedstake_v2 AS (
  SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount AS eth_amount_unadjusted,
    eth_amount_adj AS eth_amount_adjusted,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v2' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__sharedstake_v2_withdrawals') }}

{% if is_incremental() and 'sharedstake_v2' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
stader AS (
  SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount AS eth_amount_unadjusted,
    eth_amount_adj AS eth_amount_adjusted,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__stader_withdrawals') }}

{% if is_incremental() and 'stader' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
stafi AS (
  SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount AS eth_amount_unadjusted,
    eth_amount_adj AS eth_amount_adjusted,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__stafi_withdrawals') }}

{% if is_incremental() and 'stafi' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
stakewise_v3 AS (
  SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount AS eth_amount_unadjusted,
    eth_amount_adj AS eth_amount_adjusted,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v3' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__stakewise_v3_withdrawals') }}

{% if is_incremental() and 'stakewise_v3' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
unieth AS (
  SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount AS eth_amount_unadjusted,
    eth_amount_adj AS eth_amount_adjusted,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__unieth_withdrawals') }}

{% if is_incremental() and 'unieth' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
all_lsd AS (
  SELECT
    *
  FROM
    ankr
  UNION ALL
  SELECT
    *
  FROM
    cream
  UNION ALL
  SELECT
    *
  FROM
    frax
  UNION ALL
  SELECT
    *
  FROM
    lido
  UNION ALL
  SELECT
    *
  FROM
    nodedao
  UNION ALL
  SELECT
    *
  FROM
    rocketpool
  UNION ALL
  SELECT
    *
  FROM
    sharedstake
  UNION ALL
  SELECT
    *
  FROM
    sharedstake_v2
  UNION ALL
  SELECT
    *
  FROM
    stakewise_v3
  UNION ALL
  SELECT
    *
  FROM
    stader
  UNION ALL
  SELECT
    *
  FROM
    stafi
  UNION ALL
  SELECT
    *
  FROM
    unieth
  UNION ALL
  SELECT
    *
  FROM
    coinbase
),
complete_lsd AS (
  SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    sender,
    recipient,
    token_amount AS token_amount_unadj,
    token_amount_adj,
    token_amount_adj * p2.price AS token_amount_usd,
    CASE
      WHEN platform = 'coinbase'
      AND p2.price IS NULL THEN token_amount_adj
      WHEN platform = 'coinbase'
      AND p2.price IS NOT NULL THEN (
        token_amount_adj * p2.price
      ) / p1.price
      ELSE eth_amount_adjusted
    END AS eth_amount_adj,
    CASE
      WHEN platform = 'coinbase' THEN eth_amount_adj * pow(
        10,
        18
      )
      ELSE eth_amount_unadjusted
    END AS eth_amount_unadj,
    eth_amount_adj * p1.price AS eth_amount_usd,
    s.token_address,
    token_symbol,
    platform,
    version,
    _log_id,
    _inserted_timestamp
  FROM
    all_lsd s
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p1
    ON p1.token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    AND DATE_TRUNC(
      'hour',
      s.block_timestamp
    ) = p1.hour
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p2
    ON p2.token_address = s.token_address
    AND DATE_TRUNC(
      'hour',
      s.block_timestamp
    ) = p2.hour
),

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
heal_model AS (
  SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    sender,
    recipient,
    token_amount_unadj,
    token_amount_adj,
    token_amount_adj * p2.price AS token_amount_usd_heal,
    CASE
      WHEN platform = 'coinbase'
      AND p2.price IS NULL THEN token_amount_adj
      WHEN platform = 'coinbase'
      AND p2.price IS NOT NULL THEN (
        token_amount_adj * p2.price
      ) / p1.price
      ELSE eth_amount_adj
    END AS eth_amount_adj_heal,
    CASE
      WHEN platform = 'coinbase' THEN eth_amount_adj_heal * pow(
        10,
        18
      )
      ELSE eth_amount_unadj
    END AS eth_amount_unadj_heal,
    eth_amount_adj_heal * p1.price AS eth_amount_usd_heal,
    t0.token_address,
    token_symbol,
    platform,
    version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ this }}
    t0
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p1
    ON p1.token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    AND DATE_TRUNC(
      'hour',
      t0.block_timestamp
    ) = p1.hour
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p2
    ON p2.token_address = t0.token_address
    AND DATE_TRUNC(
      'hour',
      t0.block_timestamp
    ) = p2.hour
  WHERE
    CONCAT(
      t0.block_number,
      '-',
      t0.platform,
      '-',
      t0.version
    ) IN (
      SELECT
        CONCAT(
          t1.block_number,
          '-',
          t1.platform,
          '-',
          t1.version
        )
      FROM
        {{ this }}
        t1
      WHERE
        t1.eth_amount_usd IS NULL
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
            AND p.token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
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
      t0.platform,
      '-',
      t0.version
    ) IN (
      SELECT
        CONCAT(
          t2.block_number,
          '-',
          t2.platform,
          '-',
          t2.version
        )
      FROM
        {{ this }}
        t2
      WHERE
        t2.token_amount_usd IS NULL
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
            AND p.token_address = t2.token_address
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
    complete_lsd

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
UNION ALL
SELECT
  block_number,
  block_timestamp,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  tx_hash,
  event_index,
  event_name,
  contract_address,
  sender,
  recipient,
  token_amount_unadj,
  token_amount_adj,
  token_amount_usd_heal AS token_amount_usd,
  eth_amount_adj_heal AS eth_amount_adj,
  eth_amount_unadj_heal AS eth_amount_unadj,
  eth_amount_usd_heal AS eth_amount_usd,
  token_address,
  token_symbol,
  platform,
  version,
  _log_id,
  _inserted_timestamp
FROM
  heal_model
{% endif %}
)
SELECT
  block_number,
  block_timestamp,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  tx_hash,
  event_index,
  event_name,
  contract_address,
  sender,
  recipient,
  eth_amount_unadj,
  eth_amount_adj,
  eth_amount_usd,
  token_amount_unadj,
  token_amount_adj,
  token_amount_usd,
  token_address,
  token_symbol,
  platform,
  version,
  _log_id,
  _inserted_timestamp,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_hash','event_index']
  ) }} AS complete_lsd_withdrawals_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL
