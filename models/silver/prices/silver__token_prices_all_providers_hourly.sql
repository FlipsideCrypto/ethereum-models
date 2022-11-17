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
    imputed AS is_imputed,
    _inserted_timestamp
FROM
    {{ ref('silver__token_prices_coin_gecko_hourly') }} 

{% if is_incremental() %}
WHERE _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
    )
{% endif %}

UNION

SELECT
    'coinmarketcap' AS provider,
    recorded_hour AS hour,
    token_address,
    close AS price,
    imputed AS is_imputed,
    _inserted_timestamp
FROM
    {{ ref('silver__token_prices_coin_market_cap_hourly')}} 

{% if is_incremental() %}
WHERE _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
    )
{% endif %}
),

final AS (

SELECT 
    *,
    CASE
        WHEN provider = 'coingecko' AND is_imputed = FALSE THEN 1
        WHEN provider = 'coinmarketcap' AND is_imputed = FALSE THEN 2
        WHEN provider = 'coingecko' AND is_imputed = TRUE THEN 3
        WHEN provider = 'coinmarketcap' AND is_imputed = TRUE THEN 4
    END AS priority
FROM all_providers
)

SELECT
    hour,
    token_address,
    c.symbol,
    c.decimals,
    price,
    is_imputed,
    {{ dbt_utils.surrogate_key( ['hour', 'token_address'] ) }} AS _unique_key
FROM final p
LEFT JOIN {{ ref('core__dim_contracts') }} c 
    ON LOWER(c.address) = LOWER(p.token_address)
QUALIFY(ROW_NUMBER() OVER(PARTITION BY hour, token_address ORDER BY priority ASC)) = 1