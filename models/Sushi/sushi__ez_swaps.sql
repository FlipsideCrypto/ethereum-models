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
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_out,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  amount_in_usd,
  amount_out_usd,
  _log_id,
  ingested_at
FROM
  {{ ref('silver_dex__v2_swaps') }}
WHERE
  platform = 'sushiswap'

{% if is_incremental() %}
AND ingested_at >= (
  SELECT
    MAX(ingested_at)
  FROM
    {{ this }}
)
{% endif %}
