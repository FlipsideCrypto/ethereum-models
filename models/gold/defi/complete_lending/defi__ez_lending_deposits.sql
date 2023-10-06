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
  tx_hash,
  block_number,
  block_timestamp,
  event_index,
  protocol_token,
  deposit_asset,
  deposit_amount,
  deposit_amount_usd,
  depositor_address,
  lending_pool_contract,
  issued_deposit_tokens,
  platform,
  symbol
FROM 
    {{ ref('silver__complete_lending_deposits') }}