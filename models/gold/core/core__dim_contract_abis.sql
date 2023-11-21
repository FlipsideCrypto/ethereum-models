{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    contract_address,
    DATA AS abi,
    abi_source,
    bytecode,
    COALESCE (
        abis_id,
        {{ dbt_utils.generate_surrogate_key(
            ['contract_address']
        ) }}
    ) AS dim_contract_abis_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__abis') }}
