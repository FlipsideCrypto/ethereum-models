{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BEACON' } } },
    tags = ['gold','beacon']
) }}

SELECT
    request_slot_number,
    submit_slot_number,
    pubkey,
    signature,
    withdrawal_credentials,
    amount / pow(10, 9) AS amount,
    projected_processing_slot,
    projected_processing_epoch,
    projected_processing_timestamp,
    queue_position,
    total_queue_size,
    deposit_id,
    pending_deposits_id as fact_pending_deposits_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__pending_deposits') }}