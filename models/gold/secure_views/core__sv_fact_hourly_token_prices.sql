{{ config(
    materialized = 'view',
    secure = true,
    pre_hook = "call silver.sp_create_cross_db_share_clones()"
) }}

SELECT
    *
FROM
    {{ source('ethereum_share','token_prices_hourly')}}
