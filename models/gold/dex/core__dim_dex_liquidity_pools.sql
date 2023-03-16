{{ config(
    materialized = 'view',
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
    creation_block,
    creation_time,
    creation_tx,
    factory_address,
    pool_name,
    pool_address,
    platform,
    token0_address as token0,
    token0_symbol,
    token0_decimals,
    token1_address as token1,
    token1_symbol,
    token1_decimals,
    tokens
FROM
    {{ ref('silver_dex__pools') }}
