{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    date,
    block_number,
    pid,
    contract_address,
    allocation_points,
    call_name,
    function_signature
FROM
    {{ ref('silver_dex__v2_pool_monthly_metrics') }}
