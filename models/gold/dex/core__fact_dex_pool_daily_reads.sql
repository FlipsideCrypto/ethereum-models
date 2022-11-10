{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    id,
    date,
    block_number,
    contract_address,
    Balance_of_SLP_Staked,
    total_supply_of_SLP
FROM
    {{ ref('silver_dex__v2_pool_daily_metrics') }}
