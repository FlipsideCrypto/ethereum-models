{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['gold','reads']
) }}

SELECT
    contract_address,
    block_number,
    function_signature,
    function_input,
    read_output,
    segmented_data AS segmented_output,
    COALESCE (
        reads_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_number', 'contract_address', 'function_signature', 'function_input']
        ) }}
    ) AS fact_contract_reads_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__reads') }}
