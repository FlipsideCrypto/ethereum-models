{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'OLAS, AUTONOLAS, VALORY',
    'PURPOSE': 'AI, SERVICES, REGISTRY' } } }
) }}

SELECT
    r.block_number,
    r.block_timestamp,
    r.tx_hash,
    r.origin_function_signature,
    r.origin_from_address,
    r.origin_to_address,
    r.contract_address,
    r.event_index,
    r.event_name,
    r.owner_address,
    r.multisig_address,
    r.service_id,
    m.name,
    m.description,
    m.agent_ids,
    m.trait_type,
    m.trait_value,
    m.image_link,
    m.code_uri_link AS service_metadata_link,
    r.service_registration_id AS ez_service_registrations_id,
    r.inserted_timestamp,
    GREATEST(
        r.modified_timestamp,
        m.modified_timestamp
    ) AS modified_timestamp
FROM
    {{ ref('silver_olas__service_registrations') }}
    r
    LEFT JOIN {{ ref('olas__dim_registry_metadata') }}
    m
    ON r.contract_address = m.contract_address
    AND r.service_id = m.registry_id
