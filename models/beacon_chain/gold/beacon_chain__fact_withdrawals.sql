{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['gold','beacon']
) }}

SELECT
    slot_number,
    slot_timestamp,
    epoch_number,
    (withdrawal_amount / pow(10, 9)) :: FLOAT AS withdrawal_amount,
    address AS withdrawal_address,
    INDEX AS withdrawal_index,
    validator_index,
    COALESCE (
        beacon_withdrawals_id,
        {{ dbt_utils.generate_surrogate_key(
            ['slot_number', 'INDEX']
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
