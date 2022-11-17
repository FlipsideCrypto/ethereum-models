{{ config(
    materialized = 'view'
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