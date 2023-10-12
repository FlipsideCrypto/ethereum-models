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
  liquidator,
  ctokens_seized,
  collateral_ctoken,
  collateral_symbol,
  liquidation_amount,
  liquidation_amount_usd,
  liquidation_contract_address,
  liquidation_contract_symbol
FROM
  {{ ref('silver__compv2_liquidations') }}
