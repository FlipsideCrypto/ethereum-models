{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'OLAS, AUTONOLAS, VALORY',
    'PURPOSE': 'AI, SERVICES, REGISTRY' } } }
) }}

SELECT
    NAME,
    description,
    registry_id,
    contract_address,
    CASE
        WHEN contract_address = '0x9338b5153ae39bb89f50468e608ed9d764b755fd' THEN 'Service'
        WHEN contract_address = '0x2f1f7d38e4772884b88f3ecd8b6b9facdc319112' THEN 'Agent'
        WHEN contract_address = '0x15bd56669f57192a97df41a2aa8f4403e9491776' THEN 'Component'
    END AS registry_type,
    trait_type,
    trait_value,
    token_uri_link,
    image_link,
    registry_metadata_id AS dim_registry_metadata_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_olas__registry_metadata') }}