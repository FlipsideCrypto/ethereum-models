{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    enabled = false,
    cluster_by = ['hour::DATE'],
) }}

WITH all_providers AS (

    SELECT
        'coingecko' AS provider,
        recorded_hour AS HOUR,
        LOWER(token_address) AS token_address,
        CLOSE AS price,
        imputed AS is_imputed,
        _inserted_timestamp
    FROM
        {{ ref('silver__token_prices_coin_gecko_hourly') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp > (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    'coinmarketcap' AS provider,
    recorded_hour AS HOUR,
    LOWER(token_address) AS token_address,
    CLOSE AS price,
    imputed AS is_imputed,
    _inserted_timestamp
FROM
    {{ ref('silver__token_prices_coin_market_cap_hourly') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp > (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
FINAL AS (
    SELECT
        *,
        CASE
            WHEN provider = 'coingecko'
            AND is_imputed = FALSE THEN 1
            WHEN provider = 'coinmarketcap'
            AND is_imputed = FALSE THEN 2
            WHEN provider = 'coingecko'
            AND is_imputed = TRUE THEN 3
            WHEN provider = 'coinmarketcap'
            AND is_imputed = TRUE THEN 4
        END AS priority
    FROM
        all_providers
)
SELECT
    HOUR,
    token_address,
    C.symbol,
    C.decimals,
    price,
    is_imputed,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['hour', 'token_address']
    ) }} AS _unique_key
FROM
    FINAL p
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON LOWER(
        C.address
    ) = LOWER(p.token_address) qualify(ROW_NUMBER() over(PARTITION BY HOUR, token_address
ORDER BY
    priority ASC)) = 1
