{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['non_realtime']
) }}

SELECT
    address,
    symbol,
    NAME,
    decimals,
    contract_metadata
FROM
    {{ ref('silver__contracts') }}
