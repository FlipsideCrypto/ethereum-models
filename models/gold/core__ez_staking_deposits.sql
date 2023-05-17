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
    COALESCE(
        label,
        'other'
    ) AS platform,
    COALESCE(
        label_type,
        'other'
    ) AS platform_type,
    contract_address,
    COALESCE(
        address_name,
        'other'
    ) AS contract_name,
    COALESCE(
        label_subtype,
        'other'
    ) AS contract_type,
    pubkey,
    withdrawal_credentials,
    withdrawal_type,
    withdrawal_address,
    signature,
    deposit_index
FROM
    {{ ref('silver__eth_staking_deposits') }}
    d
    LEFT JOIN {{ ref('core__dim_labels') }}
    l
    ON d.platform_address = l.address
