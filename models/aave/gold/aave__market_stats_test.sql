{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_hour, aave_version, aave_market)",
    incremental_strategy = 'delete+insert',
    tags = ['snowflake', 'ethereum', 'aave', 'aave_market_stats', 'address_labels']
) }}

WITH aave_tokens AS (

    SELECT
        atoken_address,
        atoken_symbol,
        atoken_name,
        atoken_decimals,
        underlying_address,
        underlying_symbol,
        underlying_name,
        underlying_decimals,
        atoken_version,
        atoken_created_block,
        atoken_stable_debt_address,
        atoken_variable_debt_address
    FROM
        {{ ref('silver__aave_tokens') }}
),
blocks AS (
    SELECT
        block_number,
        block_timestamp,
        DATE_TRUNC(
            'hour',
            block_timestamp
        ) AS block_hour
    FROM
        {{ ref('silver__blocks') }}
),
reads_data AS (
    SELECT
        A.block_number,
        block_hour AS blockhour,
        block_timestamp,
        contract_address AS lending_pool_add,
        token_address AS reserve_token,
        _inserted_timestamp,
        availableLiquidity AS available_liquidity,
        totalStableDebt AS total_stable_debt,
        totalVariableDebt AS total_variable_debt,
        liquidityRate :: numeric / pow(
            10,
            27
        ) AS liquidity_rate,
        variableBorrowRate :: numeric / pow(
            10,
            27
        ) AS variable_borrow_rate,
        COALESCE(
            stableBorrowRate :: numeric,
            averageStableBorrowRate :: numeric
        ) / pow(
            10,
            27
        ) AS stbl_borrow_rate,
        averageStableBorrowRate,
        liquidityIndex :: numeric / pow(
            10,
            27
        ) AS utilization_rate,
        variableBorrowIndex,
        lastUpdateTimestamp,
        CASE
            WHEN contract_address IN (
                LOWER('0x057835Ad21a177dbdd3090bB1CAE03EaCF78Fc6d'),
                LOWER('0xc443AD9DDE3cecfB9dfC5736578f447aFE3590ba')
            ) THEN 'DataProvider'
            ELSE 'LendingPool'
        END AS lending_pool_type,
        CASE
            WHEN contract_address IN (
                LOWER('0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9'),
                LOWER('0x057835Ad21a177dbdd3090bB1CAE03EaCF78Fc6d')
            ) THEN 'Aave V2'
            WHEN contract_address IN (
                LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb'),
                LOWER('0xc443AD9DDE3cecfB9dfC5736578f447aFE3590ba')
            ) THEN 'Aave AMM'
            ELSE 'Aave V1'
        END AS aave_version
    FROM
        {{ ref('silver__aave_market_stats') }} A
        LEFT JOIN blocks
        ON A.block_number = blocks.block_number

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) :: DATE - 2
        FROM
            {{ this }}
    )
{% endif %}
),
reads_parsed AS (
    SELECT
        reads_data.*,
        atoken_address,
        atoken_symbol,
        atoken_name,
        atoken_decimals,
        underlying_address,
        underlying_symbol,
        underlying_name,
        underlying_decimals,
        atoken_version,
        atoken_created_block,
        atoken_stable_debt_address AS stable_debt_token_address,
        atoken_variable_debt_address AS variable_debt_token_address
    FROM
        reads_data
        LEFT JOIN aave_tokens
        ON aave_version = atoken_version
        AND reserve_token = underlying_address
),
lending_pools_v2 AS (
    SELECT
        *
    FROM
        reads_parsed
    WHERE
        lending_pool_type = 'LendingPool'
        AND aave_version <> 'Aave V1'
),
data_providers_v2 AS (
    SELECT
        *
    FROM
        reads_parsed
    WHERE
        lending_pool_type = 'DataProvider'
        AND aave_version <> 'Aave V1'
),
lending_pools_v1 AS (
    SELECT
        *
    FROM
        reads_parsed
    WHERE
        lending_pool_type = 'LendingPool'
        AND aave_version = 'Aave V1'
),
aave_v2 AS (
    SELECT
        lp.blockhour,
        lp.reserve_token,
        lp.aave_version,
        lp.lending_pool_add,
        dp.lending_pool_add AS data_provider,
        lp.atoken_address,
        lp.stable_debt_token_address,
        lp.variable_debt_token_address,
        (
            dp.available_liquidity + dp.total_stable_debt + dp.total_variable_debt
        ) AS total_liquidity,
        CASE
            WHEN lp.liquidity_rate IS NOT NULL THEN lp.liquidity_rate
            ELSE dp.liquidity_rate
        END AS liquidity_rate,
        CASE
            WHEN lp.stbl_borrow_rate IS NOT NULL THEN lp.stbl_borrow_rate
            ELSE dp.stbl_borrow_rate
        END AS stbl_borrow_rate,
        CASE
            WHEN lp.variable_borrow_rate IS NOT NULL THEN lp.variable_borrow_rate
            ELSE dp.variable_borrow_rate
        END AS variable_borrow_rate,
        dp.total_stable_debt AS total_stable_debt,
        dp.total_variable_debt AS total_variable_debt,
        CASE
            WHEN total_liquidity <> 0 THEN ((dp.total_stable_debt + dp.total_variable_debt) / total_liquidity)
            ELSE 0
        END AS utilization_rate,
        COALESCE(
            lp.underlying_decimals,
            dp.underlying_decimals
        ) AS underlying_decimals,
        COALESCE(
            lp.underlying_symbol,
            dp.underlying_symbol
        ) AS underlying_symbol
    FROM
        lending_pools_v2 lp
        LEFT OUTER JOIN data_providers_v2 dp
        ON lp.reserve_token = dp.reserve_token
        AND lp.blockhour = dp.blockhour
        AND lp.aave_version = dp.aave_version
),
-- format v1 data
aave_v1 AS (
    SELECT
        lp.blockhour,
        lp.reserve_token,
        lp.aave_version,
        lp.lending_pool_add,
        '-' AS data_provider,
        lp.atoken_address,
        lp.stable_debt_token_address,
        lp.variable_debt_token_address,
        lp.available_liquidity AS total_liquidity,
        lp.liquidity_rate,
        lp.stbl_borrow_rate,
        lp.variable_borrow_rate,
        lp.total_stable_debt,
        lp.total_variable_debt,
        lp.utilization_rate,
        underlying_decimals,
        underlying_symbol
    FROM
        lending_pools_v1 lp
),
aave AS (
    SELECT
        *
    FROM
        aave_v2
    UNION
    SELECT
        *
    FROM
        aave_v1
),
token_prices AS (
    SELECT
        prices_hour,
        underlying_address,
        AVG(hourly_price) AS hourly_price
    FROM
        {{ ref('silver__aave_token_prices') }}
    GROUP BY
        1,
        2
),
atoken_prices AS (
    SELECT
        HOUR AS atoken_hour,
        token_address AS atoken_address,
        AVG(price) AS hourly_atoken_price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        (
            token_address IN (
                SELECT
                    DISTINCT atoken_address
                FROM
                    aave_tokens
            )
            OR token_address = '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9'
        )
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                blocks
        )
    GROUP BY
        1,
        2
),
aave_data AS (
    SELECT
        DISTINCT A.blockhour AS block_hour,
        A.reserve_token,
        A.underlying_symbol AS reserve_name,
        A.aave_version,
        A.lending_pool_add,
        ap.hourly_price AS reserve_price,
        A.data_provider,
        A.atoken_address,
        A.stable_debt_token_address,
        A.variable_debt_token_address,
        A.total_liquidity / power(
            10,
            underlying_decimals
        ) AS total_liquidity_token,
        total_liquidity_token * ap.hourly_price AS total_liquidity_usd,
        A.liquidity_rate,
        A.stbl_borrow_rate,
        A.variable_borrow_rate,
        A.total_stable_debt / power(
            10,
            underlying_decimals
        ) AS total_stable_debt_token,
        total_stable_debt_token * ap.hourly_price AS total_stable_debt_usd,
        A.total_variable_debt / power(
            10,
            underlying_decimals
        ) AS total_variable_debt_token,
        total_variable_debt_token * ap.hourly_price AS total_variable_debt_usd,
        utilization_rate
    FROM
        aave A
        LEFT OUTER JOIN token_prices ap
        ON A.reserve_token = ap.underlying_address
        AND A.blockhour = ap.prices_hour
),
emissions AS (
    SELECT
        A.block_number,
        b.block_hour,
        token_address,
        emission_per_second
    FROM
        {{ ref('silver__aave_liquidity_mining') }} A
        LEFT JOIN blocks b
        ON A.block_number = b.block_number
),
aave_price AS (
    SELECT
        atoken_hour AS aave_token_hour,
        hourly_atoken_price AS aave_price
    FROM
        atoken_prices
    WHERE
        atoken_address = '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9'
)
SELECT
    DISTINCT A.block_hour,
    LOWER(
        A.reserve_token
    ) AS aave_market,
    --uses labels as a fallback, some of which have mixed case
    A.lending_pool_add,
    -- use these two for debugging reads, input the underlying token
    A.data_provider,
    --
    A.reserve_name,
    A.atoken_address,
    A.stable_debt_token_address,
    A.variable_debt_token_address,
    A.reserve_price,
    hourly_atoken_price AS atoken_price,
    A.total_liquidity_token,
    A.total_liquidity_usd,
    A.total_stable_debt_token,
    A.total_stable_debt_usd,
    A.total_variable_debt_token,
    A.total_variable_debt_usd,
    A.liquidity_rate AS supply_rate,
    A.stbl_borrow_rate AS borrow_rate_stable,
    A.variable_borrow_rate AS borrow_rate_variable,
    aave_price,
    A.utilization_rate,
    A.aave_version,
    'ethereum' AS blockchain,
    (
        (
            stable.emission_per_second * aave_price * 31536000
        ) / A.total_liquidity_usd
    ) AS stkaave_rate_supply,
    (
        (
            VARIABLE.emission_per_second * aave_price * 31536000
        ) / A.total_liquidity_usd
    ) AS stkaave_rate_variable_borrow
FROM
    aave_data A
    LEFT JOIN aave_price
    ON A.block_hour = aave_token_hour
    LEFT JOIN emissions stable
    ON stable.block_hour = A.block_hour
    AND stable.token_address = stable_debt_token_address
    LEFT JOIN emissions VARIABLE
    ON VARIABLE.block_hour = A.block_hour
    AND VARIABLE.token_address = variable_debt_token_address
    LEFT JOIN atoken_prices
    ON A.block_hour = atoken_hour
    AND A.atoken_address = atoken_prices.atoken_address
