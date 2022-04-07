{{ config(
    materialized = 'incremental',
    persist_docs ={ "relation": true,
    "columns": true },
    unique_key = '_log_id',
    cluster_by = ['ingested_at::DATE']
) }}


SELECT 
  block_number,
  block_timestamp,
  tx_hash,
  contract_address,
  event_name,
  amountIn,
  amountOut,
  amountIn_usd,
  amountOut_usd,
  token0_address,
  token1_address,
  token0_symbol,
  token1_symbol,
  sender,
  "TO",
  event_index,
  _log_id
FROM 
  {{ ref('core__ez_dex_swaps') }}
WHERE
  platform = 'sushiswap'