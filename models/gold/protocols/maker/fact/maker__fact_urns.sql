{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['non_realtime']
) }}

SELECT
    block_number,
    vault_no AS vault_number,
    urn_address,
    contract_address
FROM
    {{ ref('silver_maker__urns') }}
