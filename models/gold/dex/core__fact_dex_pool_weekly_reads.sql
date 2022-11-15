{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    Date,
    block_number,
    pid,
    contract_address,
    pool_address,
    allocation_points,
    function_name,
    function_signature
FROM
    {{ ref('silver_dex__v2_pool_weekly_metrics') }}
