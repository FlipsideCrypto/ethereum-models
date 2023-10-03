{{ config(
  materialized = 'incremental',
  unique_key = ['block_number','platform','version'],
  cluster_by = ['block_timestamp::DATE'],
  post_hook = "{{ fsc_utils.block_reorg(this, 12) }}",
  tags = ['non_realtime']
) }}

WITH contracts AS (

  SELECT
    address,
    symbol,
    NAME,
    decimals
  FROM
    {{ ref('silver__contracts') }}
),
prices AS (
  SELECT
    HOUR,
    token_address,
    price
  FROM
    {{ ref('price__ez_hourly_token_prices') }}
  WHERE
    token_address IN (
      SELECT
        DISTINCT address
      FROM
        contracts
    )
),
univ3_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    pool_address AS contract_address,
    NULL AS pool_name,
    'Swap' AS event_name,
    amount0_unadj / pow(10, COALESCE(c1.decimals, 18)) AS amount0_adjusted,
    amount1_unadj / pow(10, COALESCE(c2.decimals, 18)) AS amount1_adjusted,
    CASE
      WHEN c1.decimals IS NOT NULL THEN ROUND(
        p1.price * amount0_adjusted,
        2
      )
    END AS amount0_usd,
    CASE
      WHEN c2.decimals IS NOT NULL THEN ROUND(
        p2.price * amount1_adjusted,
        2
      )
    END AS amount1_usd,
    CASE
      WHEN amount0_unadj > 0 THEN ABS(amount0_unadj)
      ELSE ABS(amount1_unadj)
    END AS amount_in_unadj,
    CASE
      WHEN amount0_unadj > 0 THEN ABS(amount0_adjusted)
      ELSE ABS(amount1_adjusted)
    END AS amount_in,
    CASE
      WHEN amount0_unadj > 0 THEN ABS(amount0_usd)
      ELSE ABS(amount1_usd)
    END AS amount_in_usd,
    CASE
      WHEN amount0_unadj < 0 THEN ABS(amount0_unadj)
      ELSE ABS(amount1_unadj)
    END AS amount_out_unadj,
    CASE
      WHEN amount0_unadj < 0 THEN ABS(amount0_adjusted)
      ELSE ABS(amount1_adjusted)
    END AS amount_out,
    CASE
      WHEN amount0_unadj < 0 THEN ABS(amount0_usd)
      ELSE ABS(amount1_usd)
    END AS amount_out_usd,
    sender,
    recipient AS tx_to,
    event_index,
    'uniswap-v3' AS platform,
    'v3' AS version,
    CASE
      WHEN amount0_unadj > 0 THEN token0_address
      ELSE token1_address
    END AS token_in,
    CASE
      WHEN amount0_unadj < 0 THEN token0_address
      ELSE token1_address
    END AS token_out,
    CASE
      WHEN amount0_unadj > 0 THEN c1.symbol
      ELSE c2.symbol
    END AS symbol_in,
    CASE
      WHEN amount0_unadj < 0 THEN c1.symbol
      ELSE c2.symbol
    END AS symbol_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver__univ3_swaps') }}
    s
    LEFT JOIN contracts c1
    ON c1.address = s.token0_address
    LEFT JOIN contracts c2
    ON c2.address = s.token1_address
    LEFT JOIN prices p1
    ON s.token0_address = p1.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p1.hour
    LEFT JOIN prices p2
    ON s.token1_address = p2.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p2.hour

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE
    FROM
      {{ this }}
  )
{% endif %}
),
uni_sushi_v2_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    CASE
      WHEN amount0In <> 0
      AND amount1In <> 0
      AND amount0Out <> 0 THEN amount1In
      WHEN amount0In <> 0 THEN amount0In
      WHEN amount1In <> 0 THEN amount1In
    END AS amount_in_unadj,
    CASE
      WHEN amount0Out <> 0 THEN amount0Out
      WHEN amount1Out <> 0 THEN amount1Out
    END AS amount_out_unadj,
    CASE
      WHEN c1.decimals IS NOT NULL THEN amount_in_unadj / power(
        10,
        c1.decimals
      ) :: FLOAT
      ELSE amount_in_unadj
    END AS amount_in,
    CASE
      WHEN c2.decimals IS NOT NULL THEN amount_out_unadj / power(
        10,
        c2.decimals
      ) :: FLOAT
      ELSE amount_out_unadj
    END AS amount_out,
    c1.decimals AS decimals_in,
    c2.decimals AS decimals_out,
    token_in,
    token_out,
    c1.symbol AS symbol_in,
    c2.symbol AS symbol_out,
    NULL AS pool_name,
    sender,
    tx_to,
    event_index,
    platform,
    'v2' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__v2_swaps') }}
    s
    LEFT JOIN contracts c1
    ON c1.address = s.token_in
    LEFT JOIN contracts c2
    ON c2.address = s.token_out
  WHERE
    token_in IS NOT NULL
    AND token_out IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE
  FROM
    {{ this }}
)
{% endif %}
),
curve_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    s.tokens_sold AS amount_in_unadj,
    s.tokens_bought AS amount_out_unadj,
    sender,
    tx_to,
    event_index,
    platform,
    'v1' AS version,
    token_in,
    token_out,
    COALESCE(
      c1.symbol,
      s.symbol_in
    ) AS token_symbol_in,
    COALESCE(
      c2.symbol,
      s.symbol_out
    ) AS token_symbol_out,
    NULL AS pool_name,
    c1.decimals AS decimals_in,
    CASE
      WHEN decimals_in IS NOT NULL THEN s.tokens_sold / pow(
        10,
        decimals_in
      )
      ELSE s.tokens_sold
    END AS amount_in,
    c2.decimals AS decimals_out,
    CASE
      WHEN decimals_out IS NOT NULL THEN s.tokens_bought / pow(
        10,
        decimals_out
      )
      ELSE s.tokens_bought
    END AS amount_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__curve_swaps') }}
    s
    LEFT JOIN contracts c1
    ON c1.address = s.token_in
    LEFT JOIN contracts c2
    ON c2.address = s.token_out
  WHERE
    amount_out <> 0
    AND COALESCE(
      token_symbol_in,
      'null'
    ) <> COALESCE(token_symbol_out, 'null')

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE
  FROM
    {{ this }}
)
{% endif %}
),
balancer_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    NULL AS pool_name,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    amount_out_unadj,
    CASE
      WHEN decimals_in IS NULL THEN amount_in_unadj
      ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    CASE
      WHEN decimals_out IS NULL THEN amount_out_unadj
      ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v1' AS version,
    token_in,
    token_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__balancer_swaps') }}
    s
    LEFT JOIN contracts c1
    ON s.token_in = c1.address
    LEFT JOIN contracts c2
    ON s.token_out = c2.address

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE
    FROM
      {{ this }}
  )
{% endif %}
),
synthetix_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    decimals_in,
    decimals_out,
    amount_in_unadj,
    amount_out_unadj,
    CASE
      WHEN decimals_in IS NOT NULL THEN amount_in_unadj / pow(
        10,
        decimals_in
      )
      ELSE amount_in_unadj
    END AS amount_in,
    CASE
      WHEN decimals_out IS NOT NULL THEN amount_out_unadj / pow(
        10,
        decimals_out
      )
      ELSE amount_out_unadj
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    CONCAT(LEAST(symbol_in, symbol_out), '-', GREATEST(symbol_in, symbol_out)) AS pool_name,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__synthetix_swaps') }}
    s
    LEFT JOIN (
      SELECT
        synth_symbol AS synth_symbol_in,
        synth_proxy_address AS token_in,
        decimals AS decimals_in
      FROM
        {{ ref('silver__synthetix_synths_20230313') }}
    ) synths_in
    ON synths_in.synth_symbol_in = s.symbol_in
    LEFT JOIN (
      SELECT
        synth_symbol AS synth_symbol_out,
        synth_proxy_address AS token_out,
        decimals AS decimals_out
      FROM
        {{ ref('silver__synthetix_synths_20230313') }}
    ) synths_out
    ON synths_out.synth_symbol_out = s.symbol_out

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE
    FROM
      {{ this }}
  )
{% endif %}
),
fraxswap_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
      WHEN decimals_in IS NULL THEN amount_in_unadj
      ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
      WHEN decimals_out IS NULL THEN amount_out_unadj
      ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v1' AS version,
    token_in,
    token_out,
    NULL AS pool_name,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__fraxswap_swaps') }}
    s
    LEFT JOIN contracts c1
    ON s.token_in = c1.address
    LEFT JOIN contracts c2
    ON s.token_out = c2.address

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
shibaswap_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
      WHEN decimals_in IS NULL THEN amount_in_unadj
      ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
      WHEN decimals_out IS NULL THEN amount_out_unadj
      ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v1' AS version,
    token_in,
    token_out,
    NULL AS pool_name,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__shibaswap_swaps') }}
    s
    LEFT JOIN contracts c1
    ON s.token_in = c1.address
    LEFT JOIN contracts c2
    ON s.token_out = c2.address

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
dodo_v1_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
      WHEN decimals_in IS NULL THEN amount_in_unadj
      ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
      WHEN decimals_out IS NULL THEN amount_out_unadj
      ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v1' AS version,
    token_in,
    token_out,
    NULL AS pool_name,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__dodo_v1_swaps') }}
    s
    LEFT JOIN contracts c1
    ON s.token_in = c1.address
    LEFT JOIN contracts c2
    ON s.token_out = c2.address

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
dodo_v2_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
      WHEN decimals_in IS NULL THEN amount_in_unadj
      ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
      WHEN decimals_out IS NULL THEN amount_out_unadj
      ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v2' AS version,
    token_in,
    token_out,
    NULL AS pool_name,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__dodo_v2_swaps') }}
    s
    LEFT JOIN contracts c1
    ON s.token_in = c1.address
    LEFT JOIN contracts c2
    ON s.token_out = c2.address

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
hashflow_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
      WHEN decimals_in IS NULL THEN amount_in_unadj
      ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
      WHEN decimals_out IS NULL THEN amount_out_unadj
      ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v1' AS version,
    token_in,
    token_out,
    CONCAT(
      LEAST(
          COALESCE(symbol_in, CONCAT(SUBSTRING(token_in, 1, 5), '...', SUBSTRING(token_in, 39, 42))),
          COALESCE(symbol_out, CONCAT(SUBSTRING(token_out, 1, 5), '...', SUBSTRING(token_out, 39, 42)))
      ),
      '-',
      GREATEST(
          COALESCE(symbol_in, CONCAT(SUBSTRING(token_in, 1, 5), '...', SUBSTRING(token_in, 39, 42))),
          COALESCE(symbol_out, CONCAT(SUBSTRING(token_out, 1, 5), '...', SUBSTRING(token_out, 39, 42)))
      )
    ) AS pool_name,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__hashflow_swaps') }}
    s
    LEFT JOIN contracts c1
    ON s.token_in = c1.address
    LEFT JOIN contracts c2
    ON s.token_out = c2.address

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
maverick_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
      WHEN decimals_in IS NULL THEN amount_in_unadj
      ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
      WHEN decimals_out IS NULL THEN amount_out_unadj
      ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v1' AS version,
    token_in,
    token_out,
    NULL AS pool_name,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__maverick_swaps') }}
    s
    LEFT JOIN contracts c1
    ON s.token_in = c1.address
    LEFT JOIN contracts c2
    ON s.token_out = c2.address

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
kyberswap_v1_dynamic AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
      WHEN decimals_in IS NULL THEN amount_in_unadj
      ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
      WHEN decimals_out IS NULL THEN amount_out_unadj
      ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v1-dynamic' AS version,
    token_in,
    token_out,
    NULL AS pool_name,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__kyberswap_v1_dynamic_swaps') }}
    s
    LEFT JOIN contracts c1
    ON s.token_in = c1.address
    LEFT JOIN contracts c2
    ON s.token_out = c2.address

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
kyberswap_v1_static AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
      WHEN decimals_in IS NULL THEN amount_in_unadj
      ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
      WHEN decimals_out IS NULL THEN amount_out_unadj
      ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v1-static' AS version,
    token_in,
    token_out,
    NULL AS pool_name,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__kyberswap_v1_static_swaps') }}
    s
    LEFT JOIN contracts c1
    ON s.token_in = c1.address
    LEFT JOIN contracts c2
    ON s.token_out = c2.address

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
kyberswap_v2_elastic AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
      WHEN decimals_in IS NULL THEN amount_in_unadj
      ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
      WHEN decimals_out IS NULL THEN amount_out_unadj
      ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v2' AS version,
    token_in,
    token_out,
    NULL AS pool_name,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__kyberswap_v2_elastic_swaps') }}
    s
    LEFT JOIN contracts c1
    ON s.token_in = c1.address
    LEFT JOIN contracts c2
    ON s.token_out = c2.address

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
pancakeswap_v2_amm_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
      WHEN decimals_in IS NULL THEN amount_in_unadj
      ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
      WHEN decimals_out IS NULL THEN amount_out_unadj
      ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v2-amm' AS version,
    token_in,
    token_out,
    NULL AS pool_name,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__pancakeswap_v2_amm_swaps') }}
    s
    LEFT JOIN contracts c1
    ON s.token_in = c1.address
    LEFT JOIN contracts c2
    ON s.token_out = c2.address

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
pancakeswap_v2_mm_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
      WHEN decimals_in IS NULL THEN amount_in_unadj
      ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
      WHEN decimals_out IS NULL THEN amount_out_unadj
      ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v2-mm' AS version,
    token_in,
    token_out,
    CONCAT(
      LEAST(
          COALESCE(symbol_in, CONCAT(SUBSTRING(token_in, 1, 5), '...', SUBSTRING(token_in, 39, 42))),
          COALESCE(symbol_out, CONCAT(SUBSTRING(token_out, 1, 5), '...', SUBSTRING(token_out, 39, 42)))
      ),
      '-',
      GREATEST(
          COALESCE(symbol_in, CONCAT(SUBSTRING(token_in, 1, 5), '...', SUBSTRING(token_in, 39, 42))),
          COALESCE(symbol_out, CONCAT(SUBSTRING(token_out, 1, 5), '...', SUBSTRING(token_out, 39, 42)))
      )
    ) AS pool_name,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__pancakeswap_v2_mm_swaps') }}
    s
    LEFT JOIN contracts c1
    ON s.token_in = c1.address
    LEFT JOIN contracts c2
    ON s.token_out = c2.address

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
pancakeswap_v3_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    pool_address AS contract_address,
    NULL AS pool_name,
    'Swap' AS event_name,
    amount0 AS amount0_unadj,
    amount1 AS amount1_unadj,
    amount0_unadj / pow(10, COALESCE(c1.decimals, 18)) AS amount0_adjusted,
    amount1_unadj / pow(10, COALESCE(c2.decimals, 18)) AS amount1_adjusted,
    CASE
      WHEN c1.decimals IS NOT NULL THEN ROUND(
        p1.price * amount0_adjusted,
        2
      )
    END AS amount0_usd,
    CASE
      WHEN c2.decimals IS NOT NULL THEN ROUND(
        p2.price * amount1_adjusted,
        2
      )
    END AS amount1_usd,
    CASE
      WHEN amount0_unadj > 0 THEN ABS(amount0_unadj)
      ELSE ABS(amount1_unadj)
    END AS amount_in_unadj,
    CASE
      WHEN amount0_unadj > 0 THEN ABS(amount0_adjusted)
      ELSE ABS(amount1_adjusted)
    END AS amount_in,
    CASE
      WHEN amount0_unadj > 0 THEN ABS(amount0_usd)
      ELSE ABS(amount1_usd)
    END AS amount_in_usd,
    CASE
      WHEN amount0_unadj < 0 THEN ABS(amount0_unadj)
      ELSE ABS(amount1_unadj)
    END AS amount_out_unadj,
    CASE
      WHEN amount0_unadj < 0 THEN ABS(amount0_adjusted)
      ELSE ABS(amount1_adjusted)
    END AS amount_out,
    CASE
      WHEN amount0_unadj < 0 THEN ABS(amount0_usd)
      ELSE ABS(amount1_usd)
    END AS amount_out_usd,
    sender_address AS sender,
    recipient_address AS tx_to,
    event_index,
    'pancakeswap-v3' AS platform,
    'v3' AS version,
    CASE
      WHEN amount0_unadj > 0 THEN token0_address
      ELSE token1_address
    END AS token_in,
    CASE
      WHEN amount0_unadj < 0 THEN token0_address
      ELSE token1_address
    END AS token_out,
    CASE
      WHEN amount0_unadj > 0 THEN c1.symbol
      ELSE c2.symbol
    END AS symbol_in,
    CASE
      WHEN amount0_unadj < 0 THEN c1.symbol
      ELSE c2.symbol
    END AS symbol_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__pancakeswap_v3_swaps') }}
    s
    LEFT JOIN contracts c1
    ON c1.address = s.token0_address
    LEFT JOIN contracts c2
    ON c2.address = s.token1_address
    LEFT JOIN prices p1
    ON s.token0_address = p1.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p1.hour
    LEFT JOIN prices p2
    ON s.token1_address = p2.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p2.hour

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
trader_joe_v2_1_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
      WHEN decimals_in IS NULL THEN amount_in_unadj
      ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
      WHEN decimals_out IS NULL THEN amount_out_unadj
      ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v2.1' AS version,
    token_in,
    token_out,
    NULL AS pool_name,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__trader_joe_v2_1_swaps') }}
    s
    LEFT JOIN contracts c1
    ON s.token_in = c1.address
    LEFT JOIN contracts c2
    ON s.token_out = c2.address

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
--union all standard dex CTEs here (excludes amount_usd)
all_dex_standard AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    amount_in,
    amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    decimals_in,
    decimals_out,
    _log_id,
    _inserted_timestamp
  FROM
    uni_sushi_v2_swaps
  UNION ALL
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    amount_in,
    amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    token_symbol_in AS symbol_in,
    token_symbol_out AS symbol_out,
    decimals_in,
    decimals_out,
    _log_id,
    _inserted_timestamp
  FROM
    curve_swaps
  UNION ALL
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    amount_in,
    amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    decimals_in,
    decimals_out,
    _log_id,
    _inserted_timestamp
  FROM
    balancer_swaps
  UNION ALL
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    amount_in,
    amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    decimals_in,
    decimals_out,
    _log_id,
    _inserted_timestamp
  FROM
    synthetix_swaps
  UNION ALL
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    amount_in,
    amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    decimals_in,
    decimals_out,
    _log_id,
    _inserted_timestamp
  FROM
    dodo_v1_swaps
  UNION ALL
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    amount_in,
    amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    decimals_in,
    decimals_out,
    _log_id,
    _inserted_timestamp
  FROM
    dodo_v2_swaps
  UNION ALL
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    amount_in,
    amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    decimals_in,
    decimals_out,
    _log_id,
    _inserted_timestamp
  FROM
    fraxswap_swaps
  UNION ALL
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    amount_in,
    amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    decimals_in,
    decimals_out,
    _log_id,
    _inserted_timestamp
  FROM
    hashflow_swaps
  UNION ALL
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    amount_in,
    amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    decimals_in,
    decimals_out,
    _log_id,
    _inserted_timestamp
  FROM
    kyberswap_v1_dynamic
  UNION ALL
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    amount_in,
    amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    decimals_in,
    decimals_out,
    _log_id,
    _inserted_timestamp
  FROM
    kyberswap_v1_static
  UNION ALL
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    amount_in,
    amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    decimals_in,
    decimals_out,
    _log_id,
    _inserted_timestamp
  FROM
    kyberswap_v2_elastic
  UNION ALL
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    amount_in,
    amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    decimals_in,
    decimals_out,
    _log_id,
    _inserted_timestamp
  FROM
    maverick_swaps
  UNION ALL
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    amount_in,
    amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    decimals_in,
    decimals_out,
    _log_id,
    _inserted_timestamp
  FROM
    pancakeswap_v2_amm_swaps
  UNION ALL
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    amount_in,
    amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    decimals_in,
    decimals_out,
    _log_id,
    _inserted_timestamp
  FROM
    pancakeswap_v2_mm_swaps
  UNION ALL
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    amount_in,
    amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    decimals_in,
    decimals_out,
    _log_id,
    _inserted_timestamp
  FROM
    shibaswap_swaps
  UNION ALL
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    amount_in,
    amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    decimals_in,
    decimals_out,
    _log_id,
    _inserted_timestamp
  FROM
    trader_joe_v2_1_swaps
),
--union all non-standard dex CTEs here (excludes amount_usd)
all_dex_custom AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_in,
    amount_in_usd,
    amount_out_unadj,
    amount_out,
    amount_out_usd,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    _log_id,
    _inserted_timestamp
  FROM
    univ3_swaps
  UNION ALL
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_in,
    amount_in_usd,
    amount_out_unadj,
    amount_out,
    amount_out_usd,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    _log_id,
    _inserted_timestamp
  FROM
    pancakeswap_v3_swaps
),
FINAL AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_in,
    CASE
      WHEN s.decimals_in IS NOT NULL THEN ROUND(
        amount_in * p1.price,
        2
      )
      ELSE NULL
    END AS amount_in_usd,
    amount_out_unadj,
    amount_out,
    CASE
      WHEN s.decimals_out IS NOT NULL THEN ROUND(
        amount_out * p2.price,
        2
      )
      ELSE NULL
    END AS amount_out_usd,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    _log_id,
    _inserted_timestamp
  FROM
    all_dex_standard s
    LEFT JOIN prices p1
    ON s.token_in = p1.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p1.hour
    LEFT JOIN prices p2
    ON s.token_out = p2.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p2.hour
  UNION ALL
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_in,
    ROUND(
      amount_in_usd,
      2
    ) AS amount_in_usd,
    amount_out_unadj,
    amount_out,
    ROUND(
      amount_out_usd,
      2
    ) AS amount_out_usd,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    _log_id,
    _inserted_timestamp
  FROM
    all_dex_custom C
)
SELECT
  f.block_number,
  f.block_timestamp,
  f.tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  f.contract_address,
  CASE
    WHEN f.pool_name IS NULL THEN p.pool_name
    ELSE f.pool_name
  END AS pool_name,
  event_name,
  amount_in_unadj,
  amount_in,
  CASE
    WHEN amount_out_usd IS NULL
    OR ABS((amount_in_usd - amount_out_usd) / NULLIF(amount_out_usd, 0)) > 0.75
    OR ABS((amount_in_usd - amount_out_usd) / NULLIF(amount_in_usd, 0)) > 0.75 THEN NULL
    ELSE amount_in_usd
  END AS amount_in_usd,
  amount_out_unadj,
  amount_out,
  CASE
    WHEN amount_in_usd IS NULL
    OR ABS((amount_out_usd - amount_in_usd) / NULLIF(amount_in_usd, 0)) > 0.75
    OR ABS((amount_out_usd - amount_in_usd) / NULLIF(amount_out_usd, 0)) > 0.75 THEN NULL
    ELSE amount_out_usd
  END AS amount_out_usd,
  sender,
  tx_to,
  event_index,
  f.platform,
  f.version,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  f._log_id,
  f._inserted_timestamp
FROM
  FINAL f
LEFT JOIN {{ ref('silver_dex__complete_dex_liquidity_pools') }} p
  ON f.contract_address = p.pool_address
