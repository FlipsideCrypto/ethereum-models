{{ config(
    materialized = 'incremental',
    unique_key = "hour",
    cluster_by = ['hour::date'],
    tags = ['share']
) }}

SELECT
    HOUR,
    token_address,
    symbol,
    decimals,
    price,
    is_imputed
FROM
    {{ source(
        'flipside_gold_ethereum',
        'token_prices_hourly'
    ) }}
    where hour::date between '2021-12-01' and '2021-12-31'