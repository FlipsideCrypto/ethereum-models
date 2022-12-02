{{ config(
  materialized = 'incremental',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = ['Date','pool_address'],
  cluster_by = ['Date']
) }}

WITH pools AS (
  SELECT
    pool_address,
    pool_name,
    token0,
    token1,
    creation_time :: DATE AS creation_date,
    platform
  FROM
    {{ref("core__dim_dex_liquidity_pools")}}
  WHERE
    pool_address <> '0x9816f26f43c4c02df0daae1a0ba6a4dcd30b8ab7' -- an address whose token decimals on etherscan are incorrect
    and platform in ('uniswap-v2', 'sushiswap')
),

-- this IS TO identify dead pools 
pools_last_sync AS (
  SELECT
    MAX(
      block_timestamp :: DATE
    ) AS last_sync,
    contract_address
  FROM
    {{ref("silver__logs")}}
  WHERE
    topics [0] :: STRING = '0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1'
    AND contract_address IN (
      SELECT
        pool_address
      FROM
        pools
    )
  GROUP BY
    2
),

alive_pools AS (
  SELECT
    *
  FROM
    pools A
    LEFT JOIN pools_last_sync b
    ON A.pool_address = b.contract_address
  WHERE
    b.last_sync >= CURRENT_DATE - 30
),

all_day_pools AS (
  SELECT
    date_day AS DATE,
    b.pool_address,
    b.token0,
    b.token1,
    b.platform,
    b.pool_name
  FROM
    {{ref("core__dim_dates")}} a
    LEFT JOIN alive_pools b
    ON a.date_day >= b.creation_date
  WHERE
    date_day > '2020-05-04'
    AND date_day <= CURRENT_DATE

{% if is_incremental() %}
AND A.date_day >= (
  SELECT
    MAX(DATE)
  FROM
    {{ this }}
)
{% endif %}
),

reserves AS (
  SELECT
    block_timestamp,
    contract_address,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(
        SUBSTR(
          segmented_data [0] :: STRING,
          3,
          len(
            segmented_data [0]
          )
        )
      ) :: INTEGER
    ) AS reserve0,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(
        SUBSTR(
          segmented_data [1] :: STRING,
          3,
          len(
            segmented_data [1]
          )
        )
      ) :: INTEGER
    ) AS reserve1
  FROM
    {{ref("silver__logs")}}
  WHERE
    topics [0] :: STRING = '0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1'
    AND contract_address IN (
      SELECT
        pool_address
      FROM
        alive_pools
    )

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    MAX(DATE)
  FROM
    {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY block_timestamp :: DATE, contract_address
ORDER BY
  block_timestamp DESC)) = 1
),

reserves_joined_to_all_day_pools_filled AS(
  SELECT
    DATE,
    pool_address,
    token0,
    token1,
    platform,
    pool_name,
    LAST_VALUE(
      reserve0 ignore nulls
    ) over(
      PARTITION BY pool_address
      ORDER BY
        DATE rows BETWEEN unbounded preceding
        AND CURRENT ROW
    ) AS reserve0,
    LAST_VALUE(
      reserve1 ignore nulls
    ) over(
      PARTITION BY pool_address
      ORDER BY
        DATE rows BETWEEN unbounded preceding
        AND CURRENT ROW
    ) AS reserve1
  FROM
    all_day_pools A
    LEFT JOIN reserves b
    ON A.date = b.block_timestamp :: DATE
    AND A.pool_address = b.contract_address
),

decimals AS (
  SELECT
    address,
    decimals
  FROM
    {{ref("core__dim_contracts")}}
  WHERE
    decimals >= 6
),

daily_price AS (
  SELECT
    HOUR :: DATE AS DAY,
    token_address,
    symbol,
    AVG(price) AS daily_price
  FROM
    {{ref("core__fact_hourly_token_prices")}}
  WHERE
    token_address IS NOT NULL
    AND HOUR :: DATE >= '2020-05-04'

{% if is_incremental() %}
AND HOUR :: DATE IN (
  SELECT
    DISTINCT block_timestamp :: DATE
  FROM
    reserves
)
{% endif %}
GROUP BY
  1,
  2,
  3
),

balances AS (
  SELECT
    C.date,
    C.pool_address,
    C.pool_name,
    C.platform,
    C.token0,
    C.reserve0 * d.daily_price / power(
      10,
      e.decimals
    ) AS balance0_USD,
    d.symbol AS symbol0,
    C.token1,
    C.reserve1 * f.daily_price / power(
      10,
      g.decimals
    ) AS balance1_USD,
    f.symbol AS symbol1
  FROM
    reserves_joined_to_all_day_pools_filled C
    LEFT JOIN daily_price d
    ON C.date = d.day
    AND C.token0 = d.token_address
    LEFT JOIN decimals e
    ON C.token0 = e.address
    LEFT JOIN daily_price f
    ON C.date = f.day
    AND C.token1 = f.token_address
    LEFT JOIN decimals g
    ON C.token1 = g.address
)

SELECT
  DATE,
  pool_address,
  pool_name,
  platform,
  token0,
  balance0_USD,
  symbol0,
  token1,
  balance1_USD,
  symbol1,
  least(balance0_USD,balance1_USD)*2 AS pool_TVL
FROM
  balances
WHERE
  balance0_USD IS NOT NULL
  AND balance1_USD IS NOT NULL
