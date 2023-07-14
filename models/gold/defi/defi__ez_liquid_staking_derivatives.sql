{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
    'database_tags':{
        'table': {
            'PROTOCOL': 'ANKR, BINANCE, COINBASE, CREAM, FRAX, HORD, LIDO, NODEDAO, ROCKETPOOL, SHAREDSTAKE, STAFI, STAKEHOUND, STAKEWISE, SWELL, UNIETH',
            'PURPOSE': 'LIQUID STAKING, LSD'
            }
        }
    }
) }}

SELECT
  block_number,
  block_timestamp,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  tx_hash,
  event_index,
  'deposit' AS event_type,  
  contract_address, 
  recipient AS staker,
  platform,
  token_symbol,
  token_address,
  token_amount_adj AS token_amount,
  token_amount_usd,
  eth_amount_adj AS eth_amount,
  eth_amount_usd  
FROM
    {{ ref('silver_lsd__complete_lsd_deposits') }}
UNION ALL
SELECT
  block_number,
  block_timestamp,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  tx_hash,
  event_index,
  'withdrawal' AS event_type, 
  contract_address, 
  recipient AS staker,  
  platform,
  token_symbol,
  token_address,
  token_amount_adj AS token_amount,
  token_amount_usd,
  eth_amount_adj AS eth_amount,
  eth_amount_usd  
FROM
    {{ ref('silver_lsd__complete_lsd_withdrawals') }}