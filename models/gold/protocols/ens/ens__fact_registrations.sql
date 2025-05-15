{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'ENS',
    'PURPOSE': 'NFT, DOMAINS' } } },
    tags = ['gold','ens','curated']
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
    event_name,
    manager,
    owner,
    NAME AS ens_domain,
    label,
    node,
    token_id,
    resolver,
    cost,
    premium,
    expires_timestamp,
    COALESCE (
        ens_domain_registrations_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS fact_registrations_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver_ens__ens_domain_registrations') }}