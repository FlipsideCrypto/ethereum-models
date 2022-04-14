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
  contract_Address,
  event_name,
  amount_in,
  amount_out,
  sender,
  tx_to,
  event_index,
  _log_id,
  contract_name,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  ingested_at,
  amount_in_usd,
  amount_out_usd    
FROM
  {{ ref('core__ez_dex_swaps') }}
WHERE
  platform ilike '%sushi%'
    {% if is_incremental() %}
AND ingested_at >= (select MAX(ingested_at) FROM {{ this }})
    {% endif %}
    
