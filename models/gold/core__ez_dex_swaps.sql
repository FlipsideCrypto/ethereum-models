{{ config(
  materialized = 'incremental',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id',
  cluster_by = ['ingested_at::DATE']
) }}

WITH swaps_without_prices AS (

  SELECT
    swap.block_number,
    swap.block_timestamp,
    swap.tx_hash,
    swap.contract_address,
    swap.event_name,
    CASE
      WHEN swap.event_inputs :amount0In <> 0 THEN swap.event_inputs :amount0In
      WHEN swap.event_inputs :amount1In <> 0 THEN swap.event_inputs :amount1In
    END AS amountIn,
    CASE
      WHEN swap.event_inputs :amount0Out <> 0 THEN swap.event_inputs :amount0Out
      WHEN swap.event_inputs :amount1Out <> 0 THEN swap.event_inputs :amount1Out
    END AS amountOut,
    swap.event_inputs :sender :: STRING AS sender,
    swap.event_inputs :to :: STRING AS "TO",
    swap.event_index,
    swap._log_id,
    labels.label AS platform
  FROM
    {{ ref('core__fact_event_logs') }}
    swap
    LEFT JOIN {{ ref('core__dim_labels') }}
    labels
    ON swap.contract_address = labels.address
  WHERE
    swap.event_name = 'Swap'

{% if is_incremental() %}
AND ingested_at >= (
  SELECT
    MAX(
      ingested_at
    )
  FROM
    {{ this }}
)
{% endif %}
),
transfers AS (
  SELECT
    tx_hash,
    contract_address,
    event_name,
    event_inputs,
    event_index
  FROM
    {{ ref('core__fact_event_logs') }}
  WHERE
    event_name = 'Transfer'
),
swaps_with_transfers AS (
  SELECT
    swp.block_number,
    swp.block_timestamp,
    swp.tx_hash,
    swp.contract_address,
    swp.event_name,
    swp.amountIn,
    swp.amountOut,
    transfer0.contract_address AS token0_address,
    transfer1.contract_address AS token1_address,
    swp.sender,
    swp."TO",
    swp.event_index,
    swp._log_id,
    swp.platform
  FROM
    swaps_without_prices swp,
    transfers transfer0,
    transfers transfer1
  WHERE
    swp.tx_hash = transfer0.tx_hash
    AND amountIn = transfer0.event_inputs :value
    AND amountOut = transfer1.event_inputs :value
    AND transfer0.tx_hash = transfer1.tx_hash
    AND transfer1.event_index > transfer0.event_index
),
decimals AS (
  SELECT
    address AS token_address,
    decimals AS decimals
  FROM
    {{ ref('core__dim_contracts') }}
  WHERE
    contract_metadata :decimals IS NOT NULL
),
prices AS(
  SELECT
    token_address,
    HOUR,
    symbol,
    AVG(price) AS price,
    MAX(decimals) AS decimals
  FROM
    {{ ref('core__fact_hourly_token_prices') }}
  WHERE
    1 = 1

{% if is_incremental() %}
AND HOUR :: DATE >= CURRENT_DATE - 2
{% else %}
  AND HOUR :: DATE >= CURRENT_DATE - 720
{% endif %}
GROUP BY
  1,
  2,
  3
)
SELECT
  swp.block_number,
  swp.block_timestamp,
  swp.tx_hash,
  swp.contract_address,
  swp.event_name,
  swp.amountIn / power(
    10,
    d0.decimals
  ) AS amountIn,
  swp.amountOut / power(
    10,
    d1.decimals
  ) AS amountOut,
  swp.amountIn * price0.price / power(
    10,
    d0.decimals
  ) AS amountIn_usd,
  swp.amountOut * price1.price / power(
    10,
    d1.decimals
  ) AS amountOut_usd,
  swp.token0_address,
  swp.token1_address,
  price0.symbol AS token0_symbol,
  price1.symbol AS token1_symbol,
  swp.sender AS sender_address,
  swp."TO" AS to_address,
  COALESCE(
    swp.platform,
    ''
  ) AS platform,
  swp.event_index,
  swp._log_id
FROM
  swaps_with_transfers swp
  LEFT JOIN prices price0
  ON swp.token0_address = price0.token_address
  AND DATE_TRUNC(
    'hour',
    swp.block_timestamp
  ) = price0.hour
  LEFT JOIN decimals d0
  ON swp.token0_address = d0.token_address
  LEFT JOIN prices price1
  ON swp.token1_address = price1.token_address
  AND DATE_TRUNC(
    'hour',
    swp.block_timestamp
  ) = price1.hour
  LEFT JOIN decimals d1
  ON swp.token1_address = d1.token_address
