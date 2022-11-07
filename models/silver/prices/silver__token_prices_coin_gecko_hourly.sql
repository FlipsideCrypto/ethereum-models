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
                    'hourly_prices_coin_gecko'
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
        DISTINCT token_address AS token_address,
        id,
        symbol,
        platform
    FROM
        {{ source(
            'crosschain_silver',
            'asset_metadata_coin_gecko'
        ) }}
    WHERE LOWER(platform) = 'ethereum'
        OR id = 'ethereum'
),

base_date_hours_symbols AS (
    SELECT
        date_hour,
        token_address,
        id,
        symbol
    FROM
        date_hours
    CROSS JOIN asset_metadata
),

base_legacy_prices AS (
    SELECT
        DATE_TRUNC('hour', p.recorded_at) AS recorded_hour,
        m.token_address,
        p.asset_id AS id,
        LOWER(p.symbol) AS symbol,
        AVG(p.price) AS close    --returns single price if multiple prices within the 59th minute
    FROM {{ source(
            'flipside_silver',
            'prices_v2'
        ) }} p
    LEFT JOIN asset_metadata m ON m.id = p.asset_id
    WHERE
        provider = 'coingecko'
        AND MINUTE(recorded_at) = 59
        AND recorded_at::DATE < '2022-08-24'
        AND (LOWER(m.platform) = 'ethereum' OR LOWER(m.id) ='ethereum' OR LOWER(p.asset_id) = 'ethereum') 
        {% if is_incremental() %}
        AND recorded_at > (
            SELECT
                MAX(recorded_hour)
            FROM
                {{ this }}
        )
        {% endif %}
    GROUP BY 1,2,3,4
),

base_prices AS (
    SELECT
        p.recorded_hour,
        m.token_address,
        p.id,
        LOWER(m.symbol) AS symbol,
        p.close
    FROM
        {{ source(
            'crosschain_silver',
            'hourly_prices_coin_gecko'
        ) }} p 
    LEFT JOIN asset_metadata m ON m.id = p.id
    WHERE recorded_hour::DATE >= '2022-08-24'
        AND (LOWER(m.platform) = 'ethereum' OR LOWER(m.id) ='ethereum' OR LOWER(p.id) = 'ethereum')
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
        base_date_hours_symbols d
    LEFT JOIN prices p 
        ON p.recorded_hour = d.date_hour AND (p.token_address = d.token_address OR (d.token_address IS NULL AND LOWER(p.id) = 'ethereum'))
),

full_decimals AS (

SELECT
    LOWER(address) AS contract_address,
    decimals::INT AS decimals
  FROM {{ ref('silver__contracts') }}
  WHERE
    decimals NOT LIKE '%00%'
)

SELECT
    p.date_hour AS recorded_hour,
    p.token_address,
    p.id,
    p.symbol,
    d.decimals,
    COALESCE(
        p.hourly_close,
        p.imputed_close
    ) AS close,
    CASE
        WHEN p.hourly_close IS NULL THEN TRUE
        ELSE FALSE
    END AS imputed,
    concat_ws('-', recorded_hour, id, COALESCE(token_address,symbol)) AS _unique_key
FROM imputed_prices p 
LEFT JOIN full_decimals d ON d.contract_address = p.token_address
WHERE close IS NOT NULL