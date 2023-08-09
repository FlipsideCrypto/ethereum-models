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
        'crosschain_bronze',
        'legacy_prices'
    ) }}
    where hour::date between '2021-12-01' and '2021-12-31'