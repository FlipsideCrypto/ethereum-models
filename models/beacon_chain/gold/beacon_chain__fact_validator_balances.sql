{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_number AS slot_number,
    state_id,
    INDEX,
    balance,
    COALESCE (
       beacon_validators_id,
        {{ dbt_utils.generate_surrogate_key(
             ['block_number', 'index']
        ) }}
    ) AS fact_validator_balances_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__beacon_validators') }}