{{ config(
    materialized = 'view'
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
    {{ source(
        'flipside_gold_ethereum',
        'dex_liquidity_pools'
    ) }}
