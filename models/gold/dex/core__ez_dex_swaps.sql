{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'SUSHI, UNISWAP, CURVE, SYNTHETIX, BALANCER',
                'PURPOSE': 'DEX, SWAPS'
            }
        }
    }
) }}

WITH contracts AS (

    SELECT
        address,
        symbol,
        NAME,
        decimals,
        contract_metadata
    FROM
        {{ ref('core__dim_contracts') }}
    WHERE decimals IS NOT NULL
),

prices AS (

    SELECT
        hour,
        token_address,
        price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        token_address IN (
            SELECT
                DISTINCT address
            FROM
                contracts
        )
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
    pool_name,
    event_name,
    amount_in,
    amount_out,
    decimals_in,
    decimals_out,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    CASE
        WHEN decimals_in IS NOT NULL
          AND amount_in * p1.price <= 5 * amount_out * p2.price
          AND amount_out * p2.price <= 5 * amount_in * p1.price THEN amount_in * p1.price
        WHEN decimals_in IS NOT NULL
          AND decimals_out IS NULL THEN amount_in * p1.price
        ELSE NULL
    END AS amount_in_usd,
    CASE
        WHEN decimals_out IS NOT NULL
          AND amount_in * p1.price <= 5 * amount_out * p2.price
          AND amount_out * p2.price <= 5 * amount_in * p1.price THEN amount_out * p2.price
        WHEN decimals_out IS NOT NULL
          AND decimals_in IS NULL THEN amount_out * p2.price
        ELSE NULL
    END AS amount_out_usd,
    sender,
    tx_to,
    event_index,
    platform,
    _log_id
  FROM
    {{ ref('silver_dex__v2_swaps') }} s 
  LEFT JOIN prices p1
    ON token_in = p1.token_address
      AND DATE_TRUNC('hour', block_timestamp) = p1.hour
  LEFT JOIN prices p2
    ON token_out = p2.token_address
      AND DATE_TRUNC('hour', block_timestamp) = p2.hour
  WHERE token_in IS NOT NULL
    AND token_out IS NOT NULL
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
    pool_name,
    event_name,
    s.tokens_sold,
    s.tokens_bought,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    COALESCE(c1.symbol,s.symbol_in) AS token_symbol_in,
    COALESCE(c2.symbol,s.symbol_out) AS token_symbol_out,
    c1.decimals AS decimals_in,
    CASE
        WHEN decimals_in IS NOT NULL THEN s.tokens_sold / pow(
            10,
            decimals_in
        )
        ELSE s.tokens_sold
    END AS amount_in,
    CASE
        WHEN decimals_in IS NOT NULL THEN ROUND(
            amount_in * p1.price,
            2
        )
    END AS amount_in_usd,
    c2.decimals AS decimals_out,
    CASE
        WHEN decimals_out IS NOT NULL THEN s.tokens_bought / pow(
            10,
            decimals_out
        )
        ELSE s.tokens_bought
    END AS amount_out,
    CASE
        WHEN decimals_out IS NOT NULL THEN ROUND(
            amount_out * p2.price,
            2
        )
    END AS amount_out_usd,
    _log_id
  FROM
    {{ ref('silver_dex__curve_swaps') }} s
  LEFT JOIN contracts c1
    ON c1.address = s.token_in
  LEFT JOIN contracts c2
    ON c2.address = s.token_out
  LEFT JOIN prices p1
    ON p1.hour = DATE_TRUNC('hour', block_timestamp)
      AND p1.token_address = s.token_in
  LEFT JOIN prices p2
    ON p2.hour = DATE_TRUNC('hour', block_timestamp)
      AND p2.token_address = s.token_out
  WHERE amount_out <> 0
    AND COALESCE(token_symbol_in,'null') <> COALESCE(token_symbol_out,'null')
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
    pool_name,
    'Swap' AS event_name,
    CASE
      WHEN amount0_unadj > 0 THEN ABS(amount0_adjusted)
      ELSE ABS(amount1_adjusted)
    END AS amount_in,
    CASE
      WHEN amount0_unadj > 0 THEN ABS(amount0_usd)
      ELSE ABS(amount1_usd)
    END AS amount_in_usd,
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
    CASE
      WHEN amount0_unadj > 0 THEN token0_address
      ELSE token1_address
    END AS token_in,
    CASE
      WHEN amount0_unadj < 0 THEN token0_address
      ELSE token1_address
    END AS token_out,
    CASE
      WHEN amount0_unadj > 0 THEN token0_symbol
      ELSE token1_symbol
    END AS symbol_in,
    CASE
      WHEN amount0_unadj < 0 THEN token0_symbol
      ELSE token1_symbol
    END AS symbol_out,
    _log_id
  FROM
    {{ ref('silver__univ3_swaps') }}
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
    pool_name,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    CASE
        WHEN decimals_in IS NULL THEN amount_in_unadj
        ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    CASE
        WHEN decimals_in IS NOT NULL THEN ROUND(
            amount_in * p1.price,
            2
        )
    END AS amount_in_usd,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    CASE
        WHEN decimals_out IS NULL THEN amount_out_unadj
        ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    CASE
        WHEN decimals_out IS NOT NULL THEN ROUND(
            amount_out * p2.price,
            2
        )
    END AS amount_out_usd,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    _log_id
  FROM
    {{ ref('silver_dex__balancer_swaps') }} s
  LEFT JOIN contracts c1
    ON s.token_in = c1.address
  LEFT JOIN contracts c2
    ON s.token_out = c2.address
  LEFT JOIN prices p1
    ON s.token_in = p1.token_address
      AND DATE_TRUNC('hour', block_timestamp) = p1.hour
  LEFT JOIN prices p2
    ON s.token_out = p2.token_address
      AND DATE_TRUNC('hour', block_timestamp) = p2.hour
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
    pool_name,
    event_name,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    decimals_in,
    decimals_out,
    CASE
        WHEN decimals_in IS NOT NULL THEN amount_in_unadj / pow(10,decimals_in)
        ELSE amount_in_unadj
    END AS amount_in,
    amount_in * p1.price AS amount_in_usd,
    CASE
        WHEN decimals_out IS NOT NULL THEN amount_out_unadj / pow(10,decimals_out)
        ELSE amount_out_unadj
    END AS amount_out,
    amount_out * p2.price AS amount_out_usd,
    sender,
    tx_to,
    event_index,
    platform,
    _log_id
  FROM {{ ref('silver_dex__synthetix_swaps')}} s
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
  LEFT JOIN prices p1
    ON token_in = p1.token_address
      AND DATE_TRUNC('hour', block_timestamp) = p1.hour
  LEFT JOIN prices p2
    ON token_out = p2.token_address
      AND DATE_TRUNC('hour', block_timestamp) = p2.hour
  
)

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
  amount_in,
  amount_in_usd,
  amount_out,
  amount_out_usd,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  _log_id
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
  amount_in,
  amount_in_usd,
  amount_out,
  amount_out_usd,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  token_symbol_in AS symbol_in,
  token_symbol_out AS symbol_out,
  _log_id
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
  amount_in,
  amount_in_usd,
  amount_out,
  amount_out_usd,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  _log_id
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
  amount_in,
  amount_in_usd,
  amount_out,
  amount_out_usd,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  _log_id
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
  amount_in,
  amount_in_usd,
  amount_out,
  amount_out_usd,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  _log_id
FROM
  synthetix_swaps

  --filter out large price swings