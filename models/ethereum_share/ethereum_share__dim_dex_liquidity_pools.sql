{{ config(
    materialized = 'incremental',
    unique_key = "pool_address",
    cluster_by = ['pool_address'],
    tags = ['share']
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
    {{ ref('core__dim_dex_liquidity_pools') }}