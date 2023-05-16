{{ config(
    materialized = 'view',
    secure = true,
    post_hook = "{{ grant_data_share_statement('SV_DIM_DEX_LIQUIDITY_POOLS', 'VIEW') }}"
) }}

SELECT
    *
FROM
    {{ ref('silver__dex_liquidity_pools') }}
