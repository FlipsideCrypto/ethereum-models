{{ config(
    materialized = 'view',
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'SUSHI, UNISWAP, CURVE',
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
    CASE
        WHEN pool_name IS NULL AND platform = 'sushiswap' 
            THEN c0.symbol || '-' || c1.symbol || ' SLP'
        WHEN pool_name IS NULL AND platform = 'uniswap-v2' 
            THEN c0.symbol || '-' || c1.symbol || ' UNI-V2 LP'
        WHEN pool_name IS NULL AND platform = 'uniswap-v3' 
            THEN c0.symbol || '-' || c1.symbol || ' ' || fee || ' ' || tickSpacing || ' UNI-V3 LP'
        WHEN platform = 'curve' THEN pool_name
        ELSE pool_name
    END AS pool_name,
    pool_address,
    platform,
    token0,
    c0.symbol AS token0_symbol,
    c0.decimals AS token0_decimals,
    token1,
    c1.symbol AS token1_symbol,
    c1.decimals AS token1_decimals,
    tokens
FROM
    {{ ref('silver_dex__pools') }} p 
LEFT JOIN {{ ref('silver__contracts') }} c0
    ON c0.address = p.token0
LEFT JOIN {{ ref('silver__contracts') }} c1
    ON c1.address = p.token1