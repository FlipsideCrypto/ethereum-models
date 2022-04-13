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
<<<<<<<< HEAD:models/sushiswap/sushi__ez_swaps.sql
  log_id,
  contract_name,
  platform,
  token_In,
  token_out,
  symbol_In,
  symbol_out,
  amount_usdIn,
  amount_usdOut    
========
  _log_id
>>>>>>>> 3de4bf104ac48395b12a144e6653cc003835ccee:models/sushiswap/sushi__ez_sushi_swaps.sql
FROM
  {{ ref('core__ez_dex_swaps') }}
WHERE
  platform ilike '%sushi%'

