{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BEACON' } } },
    tags = ['gold','beacon']
) }}

SELECT
    request_slot_number,
    validator_index,
    withdrawable_epoch,
    withdrawable_epoch_timestamp,
    amount / pow(10, 9) AS amount,
    pending_partial_withdrawals_id as fact_pending_partial_withdrawals_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__pending_partial_withdrawals') }}

