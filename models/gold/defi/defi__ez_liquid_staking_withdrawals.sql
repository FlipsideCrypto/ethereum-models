{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
    'database_tags':{
        'table': {
            'PROTOCOL': 'ANKR, COINBASE, CREAM, FRAX, LIDO, NODEDAO, ROCKETPOOL, SHAREDSTAKE, STADER, STAFI, UNIETH',
            'PURPOSE': 'LIQUID STAKING, LSD'
            }
        }
    },
    tags = ['non_realtime']
) }}

SELECT
  block_number,
  block_timestamp,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  tx_hash,
  event_index,
  event_name, 
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