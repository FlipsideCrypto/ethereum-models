{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
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
    validator_details
FROM
    {{ ref('silver__beacon_validators') }}