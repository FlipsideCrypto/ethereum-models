{{ config(
  materialized = 'view',
  persist_docs ={ "relation": true,
  "columns": true },
  meta={
      'database_tags':{
          'table': {
              'PROTOCOL': 'SUSHI',
              'PURPOSE': 'DEFI, DEX, SWAPS'
          }
      }
  }
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
  amount_out_usd
FROM
  {{ ref('core__ez_dex_swaps') }}
WHERE
  platform = 'sushiswap'
