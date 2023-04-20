{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'SUSHI, UNISWAP, CURVE, SYNTHETIX, BALANCER',
    'PURPOSE': 'DEX' }}},
    enabled = false
) }}

SELECT
    id,
    DATE,
    block_number,
    contract_address,
    balance_of_slp_staked,
    total_supply_of_SLP
FROM
    {{ ref('silver_dex__v2_pool_daily_metrics') }}
