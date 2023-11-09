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
    "columns": true }
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
  repayed_amount_usd,
  compound_version as version,
  _inserted_timestamp,
  _log_id
FROM
  {{ ref('silver__comp_repayments') }}