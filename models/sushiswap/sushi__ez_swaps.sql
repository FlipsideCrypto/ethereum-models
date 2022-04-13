{{ config(
  materialized = 'incremental',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id',
  cluster_by = ['ingested_at::DATE']
) }}

SELECT
  block_number ,
  block_timestamp,
  tx_hash,
  contract_Address,
  event_name,
  amountIn,
  amountOut,
  sender,
  Swap_initiator,
  event_index,
  log_id,
  contract_name,
  platform,
  token_In,
  token_out,
  symbol_In,
  symbol_out,
  amount_usdIn,
  amount_usdOut    
FROM
  {{ ref('core__ez_dex_swaps') }}
WHERE
  platform ilike '%sushi%'

