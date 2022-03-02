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
        'flipside_gold',
        'token_prices_hourly'
    ) }}
