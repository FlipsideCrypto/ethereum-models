{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['non_realtime']
) }}

SELECT
    contract_address,
    DATA AS abi,
    abi_source,
    bytecode
FROM
    {{ ref('silver__abis') }}
