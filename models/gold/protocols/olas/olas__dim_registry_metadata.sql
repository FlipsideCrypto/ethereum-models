{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'OLAS, AUTONOLAS, VALORY',
    'PURPOSE': 'AI, SERVICES, REGISTRY' } } }
) }}

SELECT
    m.name,
    m.description,
    m.registry_id,
    m.contract_address,
    CASE
        WHEN contract_address = '0x9338b5153ae39bb89f50468e608ed9d764b755fd' THEN 'Service'
        WHEN contract_address = '0x2f1f7d38e4772884b88f3ecd8b6b9facdc319112' THEN 'Agent'
        WHEN contract_address = '0x15bd56669f57192a97df41a2aa8f4403e9491776' THEN 'Component'
    END AS registry_type,
    m.trait_type,
    m.trait_value,
    m.code_uri_link,
    m.image_link,
    CASE
        WHEN registry_type = 'Agent' THEN registry_id
        ELSE s.agent_id
    END AS agent_id,
    c.subcomponent_ids,
    m.registry_metadata_id AS dim_registry_metadata_id,
    m.inserted_timestamp,
    GREATEST(
        m.modified_timestamp,
        s.modified_timestamp
    ) AS modified_timestamp
FROM
    {{ ref('silver_olas__registry_metadata') }}
    m
    LEFT JOIN {{ ref('silver_olas__getservice_reads') }}
    s
    ON m.contract_address = s.contract_address
    AND m.registry_id = s.function_input
    LEFT JOIN {{ ref('silver_olas__getsubcomponents_reads') }} C
    ON m.contract_address = C.contract_address
    AND m.registry_id = C.function_input
