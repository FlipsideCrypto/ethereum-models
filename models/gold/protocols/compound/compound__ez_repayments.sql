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
  ctoken,
  ctoken_symbol,
  payer,
  repay_contract_address,
  repay_contract_symbol,
  repayed_amount,
  repayed_amount_usd
FROM
  {{ ref('silver__compv2_repayments') }}