{{ config(
    materialized = 'view',
    secure = true,
    pre_hook = "call silver.sp_create_cross_db_share_clones()",
    post_hook = "{{ grant_data_share_statement('SV_FACT_HOURLY_TOKEN_PRICES', 'VIEW') }}"
) }}

SELECT
    *
FROM
    {{ source('ethereum_share','token_prices_hourly')}}
