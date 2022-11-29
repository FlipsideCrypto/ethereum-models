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
    {{ ref(
        'silver__token_prices_all_providers_hourly'
    ) }}