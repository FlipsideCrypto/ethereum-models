{{ config(
    materialized = 'view'
) }}

SELECT
    hour,
    token_address,
    symbol,
    decimals,
    price,
    is_imputed
FROM
    {{ source(
        'crosschain',
        'ez_hourly_prices'
    ) }}
WHERE blockchain = 'ethereum'