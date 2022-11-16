{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['hour::DATE'],
) }}

WITH all_providers AS (

SELECT
    'coingecko' AS provider,
    recorded_hour AS hour,
    token_address,
    close AS price,
    imputed AS is_imputed
FROM
    {{ ref('silver__token_prices_coin_gecko_hourly') }} 
UNION
SELECT
    'coinmarketcap' AS provider,
    recorded_hour AS hour,
    token_address,
    close AS price,
    imputed AS is_imputed
FROM
    {{ ref('silver__token_prices_coin_market_cap_hourly')}} 

{% if is_incremental() %}
WHERE hour > (
    SELECT
        MAX(hour)
    FROM
        {{ this }}
    )
{% endif %}
)

SELECT 
    provider,
    hour,
    token_address,
    price,
    is_imputed,
    CASE
        WHEN provider = 'coinmarketcap' AND is_imputed = FALSE THEN 1
        WHEN provider = 'coingecko' AND is_imputed = FALSE THEN 2
        WHEN provider = 'coinmarketcap' AND is_imputed = TRUE THEN 3
        WHEN provider = 'coingecko' AND is_imputed = TRUE THEN 4
    END AS priority,
    {{ dbt_utils.surrogate_key( ['hour', 'token_address'] ) }} AS _unique_key
FROM all_providers