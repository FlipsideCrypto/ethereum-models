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
    block_hash,
    withdrawal_amount,
    withdrawal_address,
    COALESCE(
        label,
        'not labeled'
    ) AS withdrawal_address_name,
    COALESCE(
        label_type,
        'not labeled'
    ) AS withdrawal_address_category,
    COALESCE(
        label_subtype,
        'not labeled'
    ) AS withdrawal_address_type,
    withdrawals_root,
    withdrawal_index,
    validator_index,
    slot_number,
    epoch_number,
    COALESCE (
        eth_staking_withdrawals_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_number', 'withdrawal_address', 'withdrawal_index']
        ) }}
    ) AS ez_withdrawals_id,
    GREATEST(
        COALESCE (
            w.inserted_timestamp,
            '2000-01-01'
        ),
        COALESCE (
            l.inserted_timestamp,
            '2000-01-01'
        )
    ) AS inserted_timestamp,
    GREATEST(
        COALESCE (
            w.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE (
            l.modified_timestamp,
            '2000-01-01'
        )
    ) AS modified_timestamp
FROM
    {{ ref('silver__eth_staking_withdrawals') }}
    w
    LEFT JOIN {{ ref('core__dim_labels') }}
    l
    ON w.withdrawal_address = l.address
