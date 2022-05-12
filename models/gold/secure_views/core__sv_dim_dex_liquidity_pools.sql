{{ config(
    materialized = 'view',
    secure = true,
    pre_hook = "call silver.sp_create_cross_db_share_clones()"
) }}

SELECT
    *
FROM
    {{ source('ethereum_share','dex_liquidity_pools')}}
