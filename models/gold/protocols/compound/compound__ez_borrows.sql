{{ config(
    materialized = 'view',
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'COMPOUND',
                'PURPOSE': 'DEFI'
            }
        }
    },
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['non_realtime']
) }}
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  event_index,
  borrower,
  borrows_contract_address,
  borrows_contract_symbol,
  ctoken,
  ctoken_symbol,
  loan_amount,
  loan_amount_usd,
  compound_version
FROM
  {{ ref('silver__comp_borrows') }}