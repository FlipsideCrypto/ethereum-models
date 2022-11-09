{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['recorded_hour::DATE'],
) }}

WITH date_hours AS (

    SELECT
        date_hour
    FROM
        {{ source(
            'crosschain',
            'dim_date_hours'
        ) }}
    WHERE date_hour >= '2020-05-05'
        AND date_hour <= (
            SELECT
                MAX(recorded_hour)
            FROM
                {{ source(
                    'crosschain_silver',
                    'hourly_prices_coin_market_cap'
                ) }}
        )
        {% if is_incremental() %}
        AND date_hour > (
            SELECT
                MAX(recorded_hour)
            FROM
                {{ this }}
        )
        {% endif %}
),

asset_metadata AS (
    SELECT
        DISTINCT regexp_substr(regexp_replace(token_address,'^x','0x'),'0x[a-zA-Z0-9]*') AS token_address, 
        id,
        platform
    FROM
        {{ source(
            'crosschain_silver',
            'asset_metadata_coin_market_cap'
        ) }}
    WHERE LOWER(platform) = 'ethereum'
        OR id = 1027
),

base_date_hours_address AS (
    SELECT
        date_hour,
        token_address,
        id
    FROM
        date_hours
    CROSS JOIN asset_metadata
),

base_legacy_prices AS (
    SELECT
        DATE_TRUNC('hour', p.recorded_at) AS recorded_hour,
        m.token_address,
        p.asset_id AS id,
        AVG(p.price) AS close    --returns single price if multiple prices within the 59th minute
    FROM {{ source(
            'flipside_silver',
            'prices_v2'
        ) }} p
    LEFT JOIN asset_metadata m ON m.id = p.asset_id
    WHERE
        provider = 'coinmarketcap'
        AND MINUTE(recorded_at) = 59
        AND recorded_at::DATE < '2022-07-20'
        AND (LOWER(m.platform) = 'ethereum' OR p.asset_id = 1027) 
        {% if is_incremental() %}
        AND recorded_at > (
            SELECT
                MAX(recorded_hour)
            FROM
                {{ this }}
        )
        {% endif %}
    GROUP BY 1,2,3
),

base_prices AS (
    SELECT
        p.recorded_hour,
        m.token_address,
        p.id,
        p.close
    FROM
        {{ source(
            'crosschain_silver',
            'hourly_prices_coin_market_cap'
        ) }} p 
    LEFT JOIN asset_metadata m ON m.id = p.id
    WHERE recorded_hour::DATE >= '2022-07-20'
        AND (LOWER(m.platform) = 'ethereum' OR p.id = 1027)
        {% if is_incremental() %}
        AND recorded_hour > (
            SELECT
                MAX(recorded_hour)
            FROM
                {{ this }}
        )
        {% endif %}
),

prices AS (
    SELECT 
        *
    FROM 
        base_legacy_prices
    UNION
    SELECT
        *
    FROM
        base_prices
),

imputed_prices AS (
    SELECT
        d.*,
        p.close AS hourly_close,
        LAST_VALUE(
            p.close ignore nulls
        ) over (
            PARTITION BY d.token_address
            ORDER BY
                d.date_hour rows unbounded preceding
        ) AS imputed_close
    FROM
        base_date_hours_address d
    LEFT JOIN prices p 
        ON p.recorded_hour = d.date_hour 
        AND (p.token_address = d.token_address AND p.id = d.id OR (d.token_address IS NULL AND p.id = 1027))
),

final AS (

SELECT
    p.date_hour AS recorded_hour,
    p.token_address,
    p.id,
    COALESCE(
        p.hourly_close,
        p.imputed_close
    ) AS close,
    CASE
        WHEN p.hourly_close IS NULL THEN TRUE
        ELSE FALSE
    END AS imputed
FROM imputed_prices p 
WHERE close IS NOT NULL
)

SELECT 
    recorded_hour,
    token_address,
    AVG(close) AS close,
    CASE
        WHEN (CAST(ARRAY_AGG(imputed) AS string)) ILIKE '%true%' THEN TRUE
        ELSE FALSE 
    END AS imputed
FROM final
GROUP BY 1,2