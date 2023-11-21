{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'ENS',
    'PURPOSE': 'NFT, DOMAINS' } } }
) }}

SELECT
    last_registered_block,
    last_registered_timestamp,
    last_registered_tx_hash,
    last_registered_contract,
    manager,
    owner,
    set_address,
    ens_set,
    ens_domain,
    ens_subdomains,
    label,
    node,
    token_id,
    last_registered_cost,
    last_registered_premium,
    renewal_cost,
    expiration_timestamp,
    expired,
    resolver,
    profile,
    last_updated,
    COALESCE (
        ens_domain_current_records_id,
        {{ dbt_utils.generate_surrogate_key(
            ['ens_domain', 'label']
        ) }}
    ) AS ez_ens_domains_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver_ens__ens_domain_current_records') }}