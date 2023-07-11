{# {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
    'database_tags':{
        'table': {
            'PROTOCOL': 'LIDO,ROCKETPOOL,COINBASE,FRAX,STAKEWISE,STAKEHOUND',
            'PURPOSE': 'LIQUID, STAKING'
            }
        }
    }
) }}

SELECT
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    contract_address AS deposit_address,
    --address that received / processed eth deposit
    recipient AS staker,
    --EOA address that deposited eth and received liquid staking tokens in return
    eth_deposit_amount,
    -- amount of eth deposited
    native_token_amount,
    -- amount of liquid staking tokens representing staked eth amount
    eth_deposit_amount_usd,
    -- amount of eth deposited in usd
    native_token_amount_usd,
    -- amount of liquid staking tokens representing staked eth amount in usd
FROM
    {{ ref('silver__complete_lsd_deposits') }} #}
