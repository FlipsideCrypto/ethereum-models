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
  contract_address, --address that received / processed eth deposit
  recipient AS staker,  --EOA address that deposited eth and received liquid staking tokens in return
  platform,
  token_symbol,
  token_address,
  token_amount_adj AS token_amount, --amount of liquid staking tokens representing staked eth amount
  token_amount_usd,
  eth_amount_adj AS eth_amount,
  eth_amount_usd  
FROM
    {{ ref('silver_lsd__complete_lsd_deposits') }}