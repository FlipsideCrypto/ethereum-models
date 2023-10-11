{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'ENS',
    'PURPOSE': 'NFT, DOMAINS' } } }
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
    NAME,
    label,
    node,
    token_id,
    resolver,
    cost_raw,
    cost,
    premium_raw,
    premium,
    expires,
    expires_timestamp
FROM
    {{ ref('silver_ens__ens_domain_registrations') }}