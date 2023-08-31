{{ config(
    materialized = 'incremental',
    unique_key = "read_id",
    incremental_strategy = 'delete+insert',
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'AAVE',
                'PURPOSE': 'DEFI'
            }
        }
    },
    tags = ['non_realtime'],
    persist_docs ={ "relation": true,
    "columns": true }
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
      WHEN contract_address = '0x057835ad21a177dbdd3090bb1cae03eacf78fc6d' THEN 'Aave V2'
      WHEN contract_address = '0xc443ad9dde3cecfb9dfc5736578f447afe3590ba' THEN 'Aave AMM'
      WHEN contract_address = '0xc1ec30dfd855c287084bf6e14ae2fdd0246baf0d' THEN 'Aave V1'
    END AS aave_version,
    contract_address AS data_provider,
    CASE
      WHEN aave_version = 'Aave V2' THEN '0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9'
      WHEN aave_version = 'Aave V1' THEN '0x398ec7346dcd622edc5ae82352f02be94c62d119'
      WHEN aave_version = 'Aave AMM' THEN '0x7937d4799803fbbe595ed57278bc4ca21f3bffcb'
    END AS lending_pool_add
  FROM
    {{ ref('silver__aave_market_stats') }} A
    LEFT JOIN blocks
    ON A.block_number = blocks.block_number
  WHERE
    contract_address IN (
      LOWER('0x057835Ad21a177dbdd3090bB1CAE03EaCF78Fc6d'),
      LOWER('0xc443AD9DDE3cecfB9dfC5736578f447aFE3590ba'),
      LOWER('0xc1ec30dfd855c287084bf6e14ae2fdd0246baf0d')
    )

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(
      _inserted_timestamp
    ) :: DATE - 3
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
    {{ ref('price__ez_hourly_token_prices') }}
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
),
joined_aave_meta AS (
  SELECT
    block_number,
    block_hour,
    block_timestamp,
    token_address AS aave_market,
    data_provider,
    lending_pool_add,
    _inserted_timestamp,
    liquidityRate :: numeric / pow(
      10,
      27
    ) AS liquidity_rate,
    variableborrowrate :: numeric / pow(
      10,
      27
    ) AS variable_borrow_rate,
    stableborrowrate :: numeric / pow(
      10,
      27
    ) AS stable_borrow_rate,
    averagestableborrowrate :: numeric / pow(
      10,
      27
    ) AS average_stable_borrow_rate,
    liquidityindex :: numeric / pow(
      10,
      27
    ) AS utilization_rate,
    CASE
      WHEN aave_version <> 'Aave V1' THEN (
        availableliquidity + totalstabledebt + totalvariabledebt
      )
      ELSE availableliquidity
    END AS total_liquidity_unadj,
    CASE
      WHEN total_liquidity_unadj <> 0
      AND aave_version <> 'Aave V1' THEN ((totalstabledebt + totalvariabledebt) / total_liquidity_unadj)
      WHEN total_liquidity_unadj = 0 THEN 0
      ELSE utilization_rate
    END AS utilization_rate2,
    total_liquidity_unadj / pow(
      10,
      underlying_decimals
    ) AS total_liquidity_token,
    totalstabledebt / pow(
      10,
      underlying_decimals
    ) AS total_stable_debt_token,
    totalvariabledebt / pow(
      10,
      underlying_decimals
    ) AS total_variable_debt_token,
    underlying_symbol AS reserve_name,
    atoken_address,
    atoken_stable_debt_address AS stable_debt_token_address,
    atoken_variable_debt_address AS variable_debt_token_address,
    aave_version
  FROM
    aave_reads
    LEFT JOIN atoken_meta
    ON token_address = underlying_address
    AND aave_version = atoken_version
),
FINAL AS (
  SELECT
    joined_aave_meta.block_hour AS block_hour,
    joined_aave_meta.block_number AS block_number,
    aave_market,
    lending_pool_add,
    data_provider,
    reserve_name,
    joined_aave_meta.atoken_address AS atoken_address,
    stable_debt_token_address,
    variable_debt_token_address,
    hourly_price AS reserve_price,
    hourly_atoken_price AS atoken_price,
    total_liquidity_token,
    total_liquidity_token * hourly_price AS total_liquidity_usd,
    total_stable_debt_token,
    total_stable_debt_token * hourly_price AS total_stable_debt_usd,
    total_variable_debt_token,
    total_variable_debt_token * hourly_price AS total_variable_debt_usd,
    liquidity_rate AS supply_rate,
    COALESCE(
      stable_borrow_rate,
      average_stable_borrow_rate
    ) AS borrow_rate_stable,
    variable_borrow_rate AS borrow_rate_variable,
    aave_price,
    utilization_rate2 AS utilization_rate,
    aave_version,
    'ethereum' AS blockchain,
    COALESCE(
      div0(
        stable.emission_per_second * aave_price * 31536000,
        total_liquidity_usd
      ),
      0
    ) AS stkaave_rate_supply,
    COALESCE(
      div0(
        var_address.emission_per_second * aave_price * 31536000,
        total_liquidity_usd
      ),
      0
    ) AS stkaave_rate_variable_borrow,
    _inserted_timestamp,
    CONCAT(
      joined_aave_meta.block_number,
      '-',
      aave_version,
      '-',
      aave_market
    ) AS read_id
  FROM
    joined_aave_meta
    LEFT JOIN token_prices
    ON joined_aave_meta.block_hour = prices_hour
    AND aave_market = underlying_address
    LEFT JOIN aave_price
    ON joined_aave_meta.block_hour = aave_token_hour
    LEFT JOIN atoken_prices
    ON joined_aave_meta.block_hour = atoken_hour
    AND joined_aave_meta.atoken_address = atoken_prices.atoken_address
    LEFT JOIN stkaave stable
    ON stable.block_number = joined_aave_meta.block_number
    AND stable.token_address = joined_aave_meta.stable_debt_token_address
    LEFT JOIN stkaave var_address
    ON var_address.block_number = joined_aave_meta.block_number
    AND var_address.token_address = joined_aave_meta.variable_debt_token_address
)
SELECT
  *
FROM
  FINAL
WHERE
  block_hour IS NOT NULL
  AND aave_market IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY read_id
ORDER BY
  _inserted_timestamp DESC)) = 1
