{{ config(
    materialized = 'incremental',
    unique_key = "token_contract",
    tags = ['snowflake', 'ethereum', 'aave', 'aave_prices']
) }}

WITH ORACLE AS(

    SELECT
        DATE_TRUNC(
            'hour',
            block_timestamp
        ) AS block_hour,
        inputs :address :: STRING AS token_address,
        AVG(value_numeric) AS value_ethereum -- values are given in wei and need to be converted to ethereum
    FROM
        {{ source(
            'flipside_silver_ethereum',
            'reads'
        ) }}
    WHERE
        1 = 1
        AND contract_address = '0xa50ba011c48153de246e5192c8f9258a2ba79ca9' -- check if there is only one oracle

{% if is_incremental() %}
AND block_timestamp :: DATE >= CURRENT_DATE - 2
{% else %}
    AND block_timestamp :: DATE >= CURRENT_DATE - 720
{% endif %}
GROUP BY
    1,
    2
),
-- wen we don't have oracle pricces we use ethereum__token_prices_hourly as a backup
backup_prices AS(
    SELECT
        token_address,
        HOUR,
        decimals,
        symbol,
        AVG(price) AS price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        1 = 1

{% if is_incremental() %}
AND HOUR :: DATE >= CURRENT_DATE - 2
{% else %}
    AND HOUR :: DATE >= CURRENT_DATE - 720
{% endif %}
GROUP BY
    1,
    2,
    3,
    4
),
-- decimals backup
decimals_backup AS(
    SELECT
        address AS token_address,
        decimals,
        symbol,
        NAME
    FROM
        {{ ref('core__dim_contracts') }}
    WHERE
        1 = 1
        AND decimals IS NOT NULL
),
prices_hourly AS(
    SELECT
        underlying.aave_token,
        LOWER(
            underlying.token_contract
        ) AS token_contract,
        underlying.aave_version,
        (ORACLE.value_ethereum / pow(10,(18 - dc.decimals))) * eth_prices.price AS oracle_price,
        backup_prices.price AS backup_price,
        ORACLE.block_hour AS oracle_hour,
        backup_prices.hour AS backup_prices_hour,
        eth_prices.price AS eth_price,
        dc.decimals AS decimals,
        dc.symbol,
        value_ethereum
    FROM
        underlying
        LEFT JOIN ORACLE
        ON LOWER(
            underlying.token_contract
        ) = LOWER(
            ORACLE.token_address
        )
        LEFT JOIN backup_prices
        ON LOWER(
            underlying.token_contract
        ) = LOWER(
            backup_prices.token_address
        )
        AND ORACLE.block_hour = backup_prices.hour
        LEFT JOIN {{ ref('core__fact_hourly_token_prices') }}
        eth_prices
        ON ORACLE.block_hour = eth_prices.hour
        AND eth_prices.token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        LEFT JOIN decimals_backup dc
        ON LOWER(
            underlying.token_contract
        ) = LOWER(
            dc.token_address
        )
),
coalesced_prices AS(
    SELECT
        prices_hourly.decimals AS decimals,
        prices_hourly.symbol AS symbol,
        prices_hourly.aave_token AS aave_token,
        prices_hourly.token_contract AS token_contract,
        prices_hourly.aave_version AS aave_version,
        COALESCE(
            prices_hourly.oracle_price,
            prices_hourly.backup_price
        ) AS coalesced_price,
        COALESCE(
            prices_hourly.oracle_hour,
            prices_hourly.backup_prices_hour
        ) AS coalesced_hour
    FROM
        prices_hourly
),
-- daily avg price used when hourly price is missing (it happens a lot)
prices_daily_backup AS(
    SELECT
        token_address,
        symbol,
        DATE_TRUNC(
            'day',
            HOUR
        ) AS block_date,
        AVG(price) AS avg_daily_price,
        MAX(decimals) AS decimals
    FROM
        backup_prices
    WHERE
        1 = 1
    GROUP BY
        1,
        2,
        3
)
