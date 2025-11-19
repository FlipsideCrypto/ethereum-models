{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BEACON' } } },
    tags = ['gold','beacon']
) }}

SELECT
    request_slot_number,
    source_index,
    target_index,
    pending_consolidations_id as fact_pending_consolidations_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__pending_consolidations') }}

