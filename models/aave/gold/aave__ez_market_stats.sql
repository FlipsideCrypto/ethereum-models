{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_hour, aave_version, aave_market)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'ethereum', 'aave', 'aave_market_stats', 'address_labels']
) }}

WITH blocks AS (

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
aave_reads AS (
  SELECT
    A.block_number,
    block_hour,
    block_timestamp,
    contract_address,
    token_address,
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
    ) AS stable_borrow_rate,
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
    END AS aave_version,
    CASE
      WHEN aave_version = 'Aave V2' THEN '0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9'
      WHEN aave_version = 'Aave V1' THEN '0x398ec7346dcd622edc5ae82352f02be94c62d119'
      WHEN aave_version = 'Aave AMM' THEN '0x7937d4799803fbbe595ed57278bc4ca21f3bffcb'
    END AS lending_pool_add,
    CASE
      WHEN aave_version = 'Aave V2' THEN '0x057835ad21a177dbdd3090bb1cae03eacf78fc6d'
      WHEN aave_version = 'Aave AMM' THEN '0xc443ad9dde3cecfb9dfc5736578f447afe3590ba'
    END AS data_provider
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
atoken_meta AS (
  SELECT
    atoken_address,
    atoken_symbol,
    atoken_name,
    atoken_decimals,
    underlying_address,
    underlying_symbol,
    underlying_name,
    underlying_decimals,
    atoken_version AS aave_version,
    atoken_created_block,
    atoken_stable_debt_address,
    atoken_variable_debt_address
  FROM
    {{ ref('silver__aave_tokens') }}
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
          atoken_meta
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
lending_pools_v2 AS (
  SELECT
    *
  FROM
    aave_reads
  WHERE
    lending_pool_type = 'LendingPool'
    AND aave_version <> 'Aave V1'
),
data_providers_v2 AS (
  SELECT
    *
  FROM
    aave_reads
  WHERE
    lending_pool_type = 'DataProvider'
    AND aave_version <> 'Aave V1'
),
lending_pools_v1 AS (
  SELECT
    *
  FROM
    aave_reads
  WHERE
    lending_pool_type = 'LendingPool'
    AND aave_version = 'Aave V1'
),
aave_v2 AS (
  SELECT
    lp.block_hour,
    lp.block_timestamp,
    lp.block_number,
    lp.token_address AS reserve_token,
    lp.aave_version,
    lp.contract_address AS lending_pool_add,
    dp.contract_address AS data_provider,
    (
      dp.available_liquidity + dp.total_stable_debt + dp.total_variable_debt
    ) AS total_liquidity,
    CASE
      WHEN lp.liquidity_rate IS NOT NULL THEN lp.liquidity_rate
      ELSE dp.liquidity_rate
    END AS liquidity_rate,
    CASE
      WHEN lp.stable_borrow_rate IS NOT NULL THEN lp.stable_borrow_rate
      ELSE dp.stable_borrow_rate
    END AS stable_borrow_rate,
    CASE
      WHEN lp.variable_borrow_rate IS NOT NULL THEN lp.variable_borrow_rate
      ELSE dp.variable_borrow_rate
    END AS variable_borrow_rate,
    dp.total_stable_debt AS total_stable_debt,
    dp.total_variable_debt AS total_variable_debt,
    CASE
      WHEN total_liquidity <> 0 THEN ((dp.total_stable_debt + dp.total_variable_debt) / total_liquidity)
      ELSE 0
    END AS utilization_rate
  FROM
    lending_pools_v2 lp
    LEFT OUTER JOIN data_providers_v2 dp
    ON lp.token_address = dp.token_address
    AND lp.block_hour = dp.block_hour
    AND lp.aave_version = dp.aave_version
),
aave_v1 AS (
  SELECT
    lp.block_hour,
    lp.block_timestamp,
    lp.block_number,
    lp.token_address AS reserve_token,
    lp.aave_version,
    lp.contract_address AS lending_pool_add,
    NULL AS data_provider,
    lp.available_liquidity AS total_liquidity,
    lp.liquidity_rate,
    lp.stable_borrow_rate,
    lp.variable_borrow_rate,
    lp.total_stable_debt,
    lp.total_variable_debt,
    lp.utilization_rate
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
aave_data AS (
  SELECT
    DISTINCT A.block_hour,
    A.reserve_token AS aave_market,
    underlying_symbol AS reserve_name,
    A.aave_version,
    A.lending_pool_add,
    hourly_price AS reserve_price,
    A.data_provider,
    atoken_meta.atoken_address,
    atoken_stable_debt_address AS stable_debt_token_address,
    atoken_variable_debt_address AS variable_debt_token_address,
    A.total_liquidity / pow(
      10,
      underlying_decimals
    ) AS total_liquidity_token,
    total_liquidity_token * hourly_price AS total_liquidity_usd,
    liquidity_rate,
    stable_borrow_rate,
    variable_borrow_rate,
    total_stable_debt / pow(
      10,
      underlying_decimals
    ) AS total_stable_debt_token,
    total_stable_debt_token * hourly_price AS total_stable_debt_usd,
    total_variable_debt / pow(
      10,
      underlying_decimals
    ) AS total_variable_debt_token,
    total_variable_debt_token * hourly_price AS total_variable_debt_usd,
    utilization_rate,
    hourly_atoken_price AS atoken_price
  FROM
    aave A
    LEFT JOIN atoken_meta
    ON A.reserve_token = atoken_meta.underlying_address
    AND A.aave_version = atoken_meta.aave_version
    LEFT JOIN token_prices
    ON token_prices.prices_hour = A.block_hour
    AND token_prices.underlying_address = A.reserve_token
    LEFT JOIN atoken_prices
    ON atoken_prices.atoken_hour = A.block_hour
    AND atoken_prices.atoken_address = atoken_meta.atoken_address
),
stkaave AS (
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
  A.block_hour,
  aave_market,
  lending_pool_add,
  data_provider,
  reserve_name,
  atoken_address,
  stable_debt_token_address,
  variable_debt_token_address,
  reserve_price,
  atoken_price,
  total_liquidity_token,
  total_liquidity_usd,
  total_stable_debt_token,
  total_stable_debt_usd,
  total_variable_debt_token,
  total_variable_debt_usd,
  liquidity_rate AS supply_rate,
  stable_borrow_rate AS borrow_rate_stable,
  variable_borrow_rate AS borrow_rate_variable,
  aave_price,
  utilization_rate,
  aave_version,
  'ethereum' AS blockchain,
  ((stk1.emission_per_second * aave_price * 31536000) / A.total_liquidity_usd) AS stkaave_rate_supply,
  ((stk2.emission_per_second * aave_price * 31536000) / A.total_liquidity_usd) AS stkaave_rate_variable_borrow
FROM
  aave_data A
  LEFT JOIN aave_price
  ON A.block_hour = aave_token_hour
  LEFT JOIN stkaave stk1
  ON stk1.block_hour = A.block_hour
  AND stk1.token_address = stable_debt_token_address
  LEFT JOIN stkaave stk2
  ON stk2.block_hour = A.block_hour
  AND stk2.token_address = variable_debt_token_address
