{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    slot_number,
    slot_timestamp,
    epoch_number,
    (withdrawal_amount / pow(10, 9)) :: FLOAT AS withdrawal_amount,
    address as withdrawal_address,
    INDEX as withdrawal_index,
    validator_index,
    COALESCE (
       eth_staking_withdrawals_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_number', 'withdrawal_address', 'withdrawal_index']
        ) }}
    ) AS fact_withdrawals_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__beacon_withdrawals') }}
