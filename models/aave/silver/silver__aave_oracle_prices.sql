{{ config(
  materialized = 'incremental',
  unique_key = "ID",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'ethereum', 'aave', 'aave_oracle_prices']
) }}

WITH oracle_reads AS (

  SELECT
    block_number,
    contract_address AS oracle_contract,
    PUBLIC.udf_hex_to_int(
      read_output :: STRING
    ) AS value_ethereum,
    _inserted_timestamp,
    function_input AS token_address
  FROM
    {{ ref('silver__reads') }}
  WHERE
    function_signature = '0xb3596f07'
    AND read_output :: STRING <> '0x'

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(
      _inserted_timestamp
    ) :: DATE - 1
  FROM
    {{ this }}
)
{% endif %}
),
blocks AS (
  SELECT
    block_number,
    block_timestamp
  FROM
    {{ ref('silver__blocks') }}
  WHERE
    block_number IN (
      SELECT
        DISTINCT block_number
      FROM
        oracle_reads
    )
),
aave_tokens AS (
  SELECT
    underlying_address,
    underlying_symbol,
    underlying_name,
    underlying_decimals
  FROM
    {{ ref('silver__aave_tokens') }}
),
eth_prices AS (
  SELECT
    HOUR,
    AVG(price) AS eth_price
  FROM
    {{ ref('core__fact_hourly_token_prices') }}
  WHERE
    token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
  GROUP BY
    1
),
FINAL AS (
  SELECT
    A.block_number,
    block_timestamp,
    oracle_contract,
    value_ethereum,
    _inserted_timestamp,
    token_address,
    underlying_decimals,
    value_ethereum * eth_price / pow(
      10,
      18
    ) :: FLOAT AS price,
    DATE_TRUNC(
      'hour',
      block_timestamp
    ) AS block_hour,
    CONCAT(
      A.block_number,
      '-',
      token_address
    ) AS id
  FROM
    oracle_reads A
    JOIN blocks b
    ON A.block_number = b.block_number
    LEFT JOIN aave_tokens
    ON token_address = underlying_address
    LEFT JOIN eth_prices
    ON DATE_TRUNC(
      'hour',
      block_timestamp
    ) = HOUR
)
SELECT
  *
FROM
  FINAL qualify(ROW_NUMBER() over(PARTITION BY id
ORDER BY
  _inserted_timestamp DESC)) = 1
