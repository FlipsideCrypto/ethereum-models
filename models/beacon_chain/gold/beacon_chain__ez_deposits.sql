{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI' } } },
    tags = ['gold','beacon','ez']
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
        'not labeled'
    ) AS platform,
    COALESCE(
        label_type,
        'not labeled'
    ) AS platform_category,
    COALESCE(
        label_subtype,
        'not labeled'
    ) AS platform_address_type,
    contract_address,
    pubkey,
    withdrawal_credentials,
    withdrawal_type,
    withdrawal_address,
    signature,
    deposit_index,
    COALESCE (
        d.eth_staking_deposits_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS ez_deposits_id,
    GREATEST(
        COALESCE (
            d.inserted_timestamp,
            '2000-01-01'
        ),
        COALESCE (
            l.inserted_timestamp,
            '2000-01-01'
        )
    ) AS inserted_timestamp,
    GREATEST(
        COALESCE (
            d.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE (
            l.modified_timestamp,
            '2000-01-01'
        )
    ) AS modified_timestamp
FROM
    {{ ref('silver__eth_staking_deposits') }}
    d
    LEFT JOIN {{ ref('core__dim_labels') }}
    l
    ON d.platform_address = l.address
