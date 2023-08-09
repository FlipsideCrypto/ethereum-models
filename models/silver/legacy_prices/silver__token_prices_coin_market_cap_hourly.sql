{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    enabled = false,
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
    WHERE
        date_hour >= '2020-05-05'
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
        DISTINCT REGEXP_SUBSTR(REGEXP_REPLACE(token_address, '^x', '0x'), '0x[a-zA-Z0-9]*') AS token_address,
        id,
        platform
    FROM
        {{ source(
            'crosschain_silver',
            'asset_metadata_coin_market_cap'
        ) }}
    WHERE
        LOWER(platform) = 'ethereum'
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
        DATE_TRUNC(
            'hour',
            p.recorded_at
        ) AS recorded_hour,
        m.token_address,
        p.asset_id AS id,
        AVG(
            p.price
        ) AS CLOSE --returns single price if multiple prices within the 59th minute
    FROM
        {{ source(
            'crosschain_bronze',
            'legacy_prices'
        ) }}
        p
        LEFT JOIN asset_metadata m
        ON m.id = p.asset_id
    WHERE
        provider = 'coinmarketcap'
        AND MINUTE(recorded_at) = 59
        AND recorded_at :: DATE < '2022-07-20'
        AND (LOWER(m.platform) = 'ethereum'
        OR p.asset_id = 1027)

{% if is_incremental() %}
AND recorded_at > (
    SELECT
        MAX(recorded_hour)
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    1,
    2,
    3
),
base_prices AS (
    SELECT
        p.recorded_hour,
        p._inserted_timestamp,
        m.token_address,
        p.id,
        p.close
    FROM
        {{ source(
            'crosschain_silver',
            'hourly_prices_coin_market_cap'
        ) }}
        p
        LEFT JOIN asset_metadata m
        ON m.id = p.id
    WHERE
        recorded_hour :: DATE >= '2022-07-20'
        AND (LOWER(m.platform) = 'ethereum'
        OR p.id = 1027)

{% if is_incremental() %}
AND _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
prices AS (
    SELECT
        recorded_hour,
        token_address,
        id,
        CLOSE
    FROM
        base_legacy_prices
    UNION
    SELECT
        recorded_hour,
        token_address,
        id,
        CLOSE
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
        AND (
            p.token_address = d.token_address
            AND p.id = d.id
            OR (
                d.token_address IS NULL
                AND p.id = 1027
            )
        )
),
final_prices AS (
    SELECT
        p.date_hour AS recorded_hour,
        p.token_address,
        p.id,
        COALESCE(
            p.hourly_close,
            p.imputed_close
        ) AS CLOSE,
        CASE
            WHEN p.hourly_close IS NULL THEN TRUE
            ELSE FALSE
        END AS imputed
    FROM
        imputed_prices p
    WHERE
        CLOSE IS NOT NULL
),
base_timestamp AS (
    SELECT
        f.recorded_hour,
        f.token_address,
        AVG(
            f.close
        ) AS CLOSE,
        CASE
            WHEN (CAST(ARRAY_AGG(imputed) AS STRING)) ILIKE '%true%' THEN TRUE
            ELSE FALSEEND AS imputed,
            {{ dbt_utils.generate_surrogate_key(
                ['f.recorded_hour', 'f.token_address']
            ) }} AS _unique_key,
            MAX(_inserted_timestamp) AS _inserted_timestamp
            FROM
                final_prices f
                LEFT JOIN base_prices b
                ON f.recorded_hour = b.recorded_hour
                AND (
                    f.token_address = b.token_address
                    OR (
                        f.token_address IS NULL
                        AND f.id = 1027
                    )
                )
            GROUP BY
                1,
                2
        ),
        FINAL AS (
            SELECT
                recorded_hour,
                token_address,
                CLOSE,
                imputed,
                _unique_key,
                _inserted_timestamp,
                LAST_VALUE(
                    _inserted_timestamp ignore nulls
                ) over (
                    PARTITION BY token_address
                    ORDER BY
                        recorded_hour rows unbounded preceding
                ) AS imputed_timestamp
            FROM
                base_timestamp
        )
    SELECT
        recorded_hour,
        token_address,
        CLOSE,
        imputed,
        _unique_key,
        CASE
            WHEN imputed_timestamp IS NULL THEN '2022-07-19'
            ELSE COALESCE(
                _inserted_timestamp,
                imputed_timestamp
            )
        END AS _inserted_timestamp
    FROM
        FINAL
