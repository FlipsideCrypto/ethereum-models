{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['gold','maker','curated']
) }}

SELECT
    block_number,
    vault_no AS vault_number,
    urn_address,
    contract_address,
    COALESCE (
        urns_id,
        {{ dbt_utils.generate_surrogate_key(
            ['vault_no']
        ) }}
    ) AS fact_urns_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver_maker__urns') }}
