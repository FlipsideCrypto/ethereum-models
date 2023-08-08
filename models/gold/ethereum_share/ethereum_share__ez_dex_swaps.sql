{{ config(
    materialized = 'incremental',
    unique_key = "tx_hash",
    cluster_by = ['block_timestamp::date'],
    tags = ['share']
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
  symbol_out
FROM
  {{ref('core__ez_dex_swaps')}}
  where block_timestamp::date between '2021-12-01' and '2021-12-31'