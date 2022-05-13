{{ config(
    materialized = 'view',
    secure = true,
    pre_hook = "call silver.sp_create_cross_db_share_clones()",
    post_hook = "{{ grant_data_share_statement('SV_DIM_DEX_LIQUIDITY_POOLS', 'VIEW') }}"
) }}

SELECT
    *
FROM
    {{ source('ethereum_share','dex_liquidity_pools')}}
