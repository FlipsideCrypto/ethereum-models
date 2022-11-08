{{ config(
    materialized = 'view'
) }}

WITH all_providers AS (

SELECT
    'coingecko' AS provider,
    recorded_hour AS hour,
    token_address,
    UPPER(symbol) AS symbol,
    decimals,
    close AS price,
    imputed AS is_imputed
FROM
    {{ ref('silver__token_prices_coin_gecko_hourly') }}
UNION
SELECT
    'coinmarketcap' AS provider,
    recorded_hour AS hour,
    token_address,
    UPPER(symbol) AS symbol,
    decimals,
    close AS price,
    imputed AS is_imputed
FROM
    {{ ref('silver__token_prices_coin_market_cap_hourly') }}
)

SELECT
    hour,
    token_address,
    symbol,
    decimals,
    AVG(price) AS price,
    is_imputed
FROM all_providers
GROUP BY
    hour,
    token_address,
    symbol,
    decimals,
    is_imputed