{# {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    contract_address, --address that received / processed eth deposit
    depositor, --address that deposited eth
    eth_deposit_amount, -- amount of eth deposited
    staking_fee, -- fee paid in eth charged by protocol for staking
    staked_eth_amount, -- amount of eth deposited net fee
    native_token_amount -- amount of liquid staking tokens representing staked eth amount
FROM {{ ref('silver__complete_liquid_staking_deposits') }} #}