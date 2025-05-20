{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['gold','beacon']
) }}

SELECT
    block_number,
    block_number AS slot_number,
    state_id,
    INDEX,
    balance,
    validator_status,
    activation_eligibility_epoch,
    activation_epoch,
    effective_balance,
    exit_epoch,
    pubkey,
    slashed,
    withdrawable_epoch,
    withdrawal_credentials,
    validator_details,
    COALESCE (
       beacon_validators_id,
        {{ dbt_utils.generate_surrogate_key(
             ['block_number', 'index']
        ) }}
    ) AS fact_validators_id,
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