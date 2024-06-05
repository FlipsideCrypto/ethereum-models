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
    multisig_address,
    event_index,
    event_name,
    decoded_flat,
    service_id,
    NAME,
    description,
    service_activity_id AS ez_service_activity_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_olas__service_activity') }}
