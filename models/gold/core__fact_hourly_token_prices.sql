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
    {{ ref('silver__prices') }}