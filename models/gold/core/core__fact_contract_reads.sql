{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    contract_address,
    block_number,
    function_signature,
    function_input,
    read_output,
    segmented_data AS segmented_output
FROM
    {{ ref('silver__reads') }}
