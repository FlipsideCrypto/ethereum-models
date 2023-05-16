{{ config(
    materialized = 'view',
    secure = true,
    post_hook = "{{ grant_data_share_statement('SV_DIM_DEX_LIQUIDITY_POOLS', 'VIEW') }}"
) }}

SELECT
    creation_time,
    creation_tx,
    factory_address,
    pool_name,
    pool_address,  
    token0,
    token1,
    platform,
    tokens
FROM
    {{ ref('silver_dex__pools') }}
