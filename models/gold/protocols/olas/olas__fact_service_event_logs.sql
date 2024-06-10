{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'OLAS, AUTONOLAS, VALORY',
    'PURPOSE': 'AI, SERVICES' } } }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_index,
    multisig_address,
    service_id,
    topic_0,
    topic_1,
    topic_2,
    topic_3,
    data,
    segmented_data,
    service_event_logs_id AS fact_service_event_logs_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_olas__service_event_logs') }}