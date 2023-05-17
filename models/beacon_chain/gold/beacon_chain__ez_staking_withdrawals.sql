{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI' } } }
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
    validator_index
FROM
    {{ ref('silver__eth_staking_withdrawals') }}
    w
    LEFT JOIN {{ ref('core__dim_labels') }}
    l
    ON w.withdrawal_address = l.address
