{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
    'database_tags':{
        'table': {
            'PROTOCOL': 'ANKR, BINANCE, COINBASE, CREAM, FRAX, HORD, LIDO, NODEDAO, ROCKETPOOL, SHAREDSTAKE, STADER, STAFI, STAKEHOUND, STAKEWISE, SWELL, UNIETH',
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
  event_name, 
  contract_address, 
  recipient AS staker,
  platform,
  token_symbol,
  token_address,
  token_amount_unadj,
  token_amount_adj AS token_amount,
  token_amount_usd,
  eth_amount_unadj,
  eth_amount_adj AS eth_amount,
  eth_amount_usd,
    COALESCE (
        complete_lsd_deposits_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS ez_liquid_staking_deposits_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp  
FROM
    {{ ref('silver_lsd__complete_lsd_deposits') }}