{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_hour, aave_version, aave_market)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'ethereum', 'aave', 'aave_market_stats', 'address_labels']
) }}

WITH aave_reads AS (

  SELECT
    block_number,
    contract_address,
    token_address,
    _inserted_timestamp,
    availableLiquidity,
    totalStableDebt,
    totalVariableDebt,
    liquidityRate,
    variableBorrowRate,
    stableBorrowRate,
    averageStableBorrowRate,
    liquidityIndex,
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
    {{ ref('silver__aave_market_stats') }}

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
  WHERE
    block_number IN (
      SELECT
        DISTINCT block_number
      FROM
        aave_reads
    )
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
    token_address IN (
      SELECT
        DISTINCT atoken_address
      FROM
        atoken_meta
    )
    AND HOUR :: DATE IN (
      SELECT
        DISTINCT block_timestamp :: DATE
      FROM
        blocks
    )
)
SELECT
  A.block_number,
  block_timestamp,
  block_hour,
  atoken_address AS aave_market,
  CASE
    WHEN A.aave_version = 'Aave V2' THEN '0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9'
    WHEN A.aave_version = 'Aave V1' THEN '0x398ec7346dcd622edc5ae82352f02be94c62d119'
    WHEN A.aave_version = 'Aave AMM' THEN '0x7937d4799803fbbe595ed57278bc4ca21f3bffcb'
  END AS lending_pool_add,
  CASE
    WHEN A.aave_version = 'Aave V2' THEN '0x057835ad21a177dbdd3090bb1cae03eacf78fc6d'
    WHEN A.aave_version = 'Aave AMM' THEN '0xc443ad9dde3cecfb9dfc5736578f447afe3590ba'
  END AS data_provider,
  underlying_symbol AS reserve_name,
  atoken_address,
  atoken_stable_debt_address AS stable_debt_token_address,
  atoken_variable_debt_address AS variable_debt_token_address,
  hourly_price AS reserve_price,
  hourly_atoken_price AS atoken_price,
  availableLiquidity / pow(
    10,
    underlying_decimals
  ) AS total_liquidity_token
FROM
  aave_reads A
  LEFT JOIN blocks b
  ON A.block_number = b.block_number
  LEFT JOIN atoken_meta C
  ON A.token_address = C.underlying_address
  AND A.aave_version = C.aave_version
  LEFT JOIN token_prices
  ON token_prices.prices_hour = b.block_hour
  AND token_prices.underlying_address = C.underlying_address
  LEFT JOIN atoken_prices
  ON atoken_prices.atoken_hour = b.block_hour
  AND atoken_prices.atoken_address = C.atoken_address
