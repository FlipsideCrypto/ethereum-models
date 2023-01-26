{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'SUSHI, UNISWAP, CURVE, SYNTHETIX, BALANCER',
                'PURPOSE': 'DEX'
            }
        }
    }
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
