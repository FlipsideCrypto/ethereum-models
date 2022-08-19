{{ config(
  materialized = 'incremental',
  sort = 'block_hour',
  unique_key = 'block_hour',
  incremental_strategy = 'delete+insert',
  tags = ['compound']
) }}
-- pull all ctoken addresses and corresponding name
WITH asset_details AS (

  SELECT
    ctoken_address,
    ctoken_symbol,
    ctoken_name,
    ctoken_decimals,
    underlying_asset_address,
    ctoken_metadata,
    underlying_name,
    underlying_symbol,
    underlying_decimals,
    underlying_contract_metadata
  FROM
    {{ ref('compound__ez_asset_details') }}
),
--pull hourly prices for each undelrying
prices AS (
  SELECT
    HOUR AS block_hour,
    token_address AS token_contract,
    ctoken_address,
    underlying_decimals AS token_decimals,
    underlying_symbol AS token_symbol,
    underlying_name AS token_name,
    AVG(price) AS token_price
  FROM
    {{ ref('core__fact_hourly_token_prices') }}
    INNER JOIN asset_details
    ON token_address = asset_details.underlying_asset_address

{% if is_incremental() %}
WHERE
  HOUR >= getdate() - INTERVAL '3 days'
{% endif %}
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6
),
-- all the ingredients for the supply, borrows, and reserve market data
ingreds AS (
  SELECT
    DISTINCT DATE_TRUNC(
      'hour',
      block_timestamp
    ) AS block_hour,
    contract_address AS address,
    token_contract,
    contract_name,
    function_name,
    prices.token_price,
    prices.token_decimals,
    prices.token_symbol AS underlying_symbol,
    LAST_VALUE(value_numeric) over (
      PARTITION BY block_hour,
      address,
      contract_name,
      function_name
      ORDER BY
        block_timestamp RANGE BETWEEN unbounded preceding
        AND unbounded following
    ) AS num
  FROM
    {{ source(
      'flipside_silver_ethereum',
      'reads'
    ) }}
    rds
    INNER JOIN prices
    ON DATE_TRUNC(
      'hour',
      rds.block_timestamp
    ) = prices.block_hour
    AND rds.contract_address = prices.ctoken_address
  WHERE
    rds.contract_address IN (
      SELECT
        ctoken_address
      FROM
        asset_details
    )
    AND function_name IN (
      'exchangeRateStored',
      'totalReserves',
      'totalBorrows',
      'totalSupply',
      'decimals'
    )

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '2 days'
{% endif %}
),
-- market data with usd-equivalents based on prices and exchange rates
markets AS (
  SELECT
    block_hour,
    contract_name,
    address AS ctoken_address,
    token_contract AS underlying_contract,
    underlying_symbol,
    token_price,
    "'decimals'" AS ctoken_decimals,
    token_decimals,
    (
      "'exchangeRateStored'" / pow(10, 18 + (token_decimals - ctoken_decimals))
    ) * token_price AS ctoken_price,
    "'totalReserves'" / pow(
      10,
      token_decimals
    ) AS reserves_token_amount,
    "'totalBorrows'" / pow(
      10,
      token_decimals
    ) AS borrows_token_amount,
    "'totalSupply'" / pow(
      10,
      ctoken_decimals
    ) AS supply_token_amount,
    supply_token_amount * ctoken_price AS supply_usd,
    reserves_token_amount * token_price AS reserves_usd,
    borrows_token_amount * token_price AS borrows_usd
  FROM
    ingreds pivot(MAX(num) for function_name IN ('exchangeRateStored', 'totalReserves', 'totalBorrows', 'totalSupply', 'decimals')) AS p
),
-- comp emitted by ctoken by hour
comptr AS (
  SELECT
    DISTINCT DATE_TRUNC(
      'hour',
      erd.block_timestamp
    ) AS blockhour,
    LOWER(REGEXP_REPLACE(inputs :c_token_address, '\"', '')) AS ctoken_address,
    SUM(
      erd.value_numeric / 1e18
    ) AS comp_speed,
    p.token_price AS comp_price,
    comp_price * comp_speed AS comp_speed_usd
  FROM
    {{ source(
      'flipside_silver_ethereum',
      'reads'
    ) }}
    erd
    JOIN (
      SELECT
        *
      FROM
        prices
      WHERE
        token_symbol = 'COMP'
    ) p
    ON DATE_TRUNC(
      'hour',
      erd.block_timestamp
    ) = p.block_hour
  WHERE
    contract_address = '0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b'
    AND function_name IN ('compSpeeds')

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '2 days'
{% endif %}
GROUP BY
  1,
  2,
  4
),
-- comp emitted by ctoken by hour - borrow
comptr_borrow AS (
  SELECT
    DISTINCT DATE_TRUNC(
      'hour',
      erd.block_timestamp
    ) AS blockhour,
    LOWER(REGEXP_REPLACE(inputs :c_token_address, '\"', '')) AS ctoken_address,
    SUM(
      erd.value_numeric / 1e18
    ) AS comp_speed,
    p.token_price AS comp_price,
    comp_price * comp_speed AS comp_speed_usd
  FROM
    {{ source(
      'flipside_silver_ethereum',
      'reads'
    ) }}
    erd
    JOIN (
      SELECT
        *
      FROM
        prices
      WHERE
        token_symbol = 'COMP'
    ) p
    ON DATE_TRUNC(
      'hour',
      erd.block_timestamp
    ) = p.block_hour
  WHERE
    contract_address = '0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b'
    AND function_name IN ('compBorrowSpeeds')

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '2 days'
{% else %}
  AND block_timestamp >= getdate() - INTERVAL '9 months'
{% endif %}
GROUP BY
  1,
  2,
  4
),
-- comp emitted by ctoken by hour
comptr_supply AS (
  SELECT
    DISTINCT DATE_TRUNC(
      'hour',
      erd.block_timestamp
    ) AS blockhour,
    LOWER(REGEXP_REPLACE(inputs :c_token_address, '\"', '')) AS ctoken_address,
    SUM(
      erd.value_numeric / 1e18
    ) AS comp_speed,
    p.token_price AS comp_price,
    comp_price * comp_speed AS comp_speed_usd
  FROM
    {{ source(
      'flipside_silver_ethereum',
      'reads'
    ) }}
    erd
    JOIN (
      SELECT
        *
      FROM
        prices
      WHERE
        token_symbol = 'COMP'
    ) p
    ON DATE_TRUNC(
      'hour',
      erd.block_timestamp
    ) = p.block_hour
  WHERE
    contract_address = '0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b'
    AND function_name IN ('compSupplySpeeds')

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '2 days'
{% else %}
  AND block_timestamp >= getdate() - INTERVAL '9 months'
{% endif %}
GROUP BY
  1,
  2,
  4
),
-- supply APY
supply AS (
  SELECT
    DATE_TRUNC(
      'hour',
      block_timestamp
    ) AS blockhour,
    contract_address AS ctoken_address,
    (
      (
        (
          power(AVG(value_numeric) / 1e18 * ((60 / 13.15) * 60 * 24) + 1, 365)
        )
      ) - 1
    ) AS apy
  FROM
    {{ source(
      'flipside_silver_ethereum',
      'reads'
    ) }}
  WHERE
    function_name = 'supplyRatePerBlock'
    AND project_name = 'compound'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '2 days'
{% else %}
  AND block_timestamp >= getdate() - INTERVAL '9 months'
{% endif %}
GROUP BY
  1,
  2
),
-- borrow APY
borrow AS (
  SELECT
    DATE_TRUNC(
      'hour',
      block_timestamp
    ) AS blockhour,
    contract_address AS ctoken_address,
    (
      (
        (
          power(AVG(value_numeric) / 1e18 * ((60 / 13.15) * 60 * 24) + 1, 365)
        )
      ) - 1
    ) AS apy
  FROM
    {{ source(
      'flipside_silver_ethereum',
      'reads'
    ) }}
  WHERE
    function_name = 'borrowRatePerBlock'
    AND project_name = 'compound'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '2 days'
{% else %}
  AND block_timestamp >= getdate() - INTERVAL '9 months'
{% endif %}
GROUP BY
  1,
  2
)
SELECT
  A.block_hour,
  A.contract_name,
  A.ctoken_address,
  CASE
    WHEN A.underlying_contract = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN NULL
    ELSE A.underlying_contract
  END AS underlying_contract,
  CASE
    WHEN A.underlying_contract = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN 'ETH'
    ELSE A.underlying_symbol
  END AS underlying_symbol,
  A.token_price,
  --a.ctoken_decimals,
  --a.token_decimals,
  A.ctoken_price,
  A.reserves_token_amount,
  A.borrows_token_amount,
  A.supply_token_amount,
  A.supply_usd,
  A.reserves_usd,
  A.borrows_usd,
  b.comp_speed,
  supply.apy AS supply_apy,
  borrow.apy AS borrow_apy,
  b.comp_price,
  b.comp_speed_usd,
  CASE
    WHEN borrows_usd != 0 THEN power(
      (
        1 + (
          (
            COALESCE(NULLIF(b.comp_speed_usd, 0), b_borrow.comp_speed_usd) * 24
          ) / borrows_usd
        )
      ),
      365
    ) -1
    ELSE NULL
  END AS comp_apy_borrow,
  CASE
    WHEN supply_usd != 0 THEN power(
      (
        1 + (
          (
            COALESCE(NULLIF(b.comp_speed_usd, 0), b_supply.comp_speed_usd) * 24
          ) / supply_usd
        )
      ),
      365
    ) -1
    ELSE NULL
  END AS comp_apy_supply
FROM
  markets A
  JOIN comptr b
  ON A.ctoken_address = b.ctoken_address
  AND A.block_hour = b.blockhour
  JOIN comptr_borrow b_borrow
  ON A.ctoken_address = b_borrow.ctoken_address
  AND A.block_hour = b_borrow.blockhour
  JOIN comptr_supply b_supply
  ON A.ctoken_address = b_supply.ctoken_address
  AND A.block_hour = b_supply.blockhour
  LEFT JOIN supply
  ON A.ctoken_address = supply.ctoken_address
  AND A.block_hour = supply.blockhour
  LEFT JOIN borrow
  ON A.ctoken_address = borrow.ctoken_address
  AND A.block_hour = borrow.blockhour
WHERE
  comp_apy_borrow < 1000000
  AND comp_apy_supply < 1000000
