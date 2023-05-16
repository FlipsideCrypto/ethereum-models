{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI' } } }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    deposit_amount,
    depositor,
    deposit_address,
    platform_address,
    platform,
    platform_type,
    contract_address,
    contract_name,
    contract_type,
    pubkey,
    withdrawal_credentials,
    withdrawal_type,
    withdrawal_address,
    signature,
    deposit_index
FROM
    {{ ref('silver__eth_staking_deposits') }}
