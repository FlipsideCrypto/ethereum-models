{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'OLAS, AUTONOLAS, VALORY',
    'PURPOSE': 'AI, AGENTS, COMPONENTS, UNITS' } } }
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
    r.unit_id,
    r.u_type,
    r.unit_type,
    r.unit_hash,
    m.name,
    m.description,
    m.subcomponent_ids,
    m.trait_type,
    m.trait_value,
    m.image_link,
    m.code_uri_link AS unit_metadata_link,
    r.unit_registration_id AS ez_unit_registrations_id,
    r.inserted_timestamp,
    GREATEST(
        COALESCE(
            r.modified_timestamp,
            '1970-01-01' :: TIMESTAMP
        ),
        COALESCE(
            m.modified_timestamp,
            '1970-01-01' :: TIMESTAMP
        )
    ) AS modified_timestamp
FROM
    {{ ref('silver_olas__unit_registrations') }}
    r
    LEFT JOIN {{ ref('olas__dim_registry_metadata') }}
    m
    ON r.contract_address = m.contract_address
    AND r.unit_id = m.registry_id
