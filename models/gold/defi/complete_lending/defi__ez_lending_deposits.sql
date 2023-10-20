{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'COMPOUND, SPARK, AAVE, FRAXLEND',
                'PURPOSE': 'LENDING, DEPOSITS'
            }
        }
    }
) }}

SELECT
  block_number,
  block_timestamp,
  tx_hash,
  event_index,
  event_name
  protocol_market,
  deposit_asset,
  deposit_amount,
  deposit_amount_usd,
  symbol as deposit_symbol,
  depositor_address,
  platform
FROM 
    {{ ref('silver__complete_lending_deposits') }}