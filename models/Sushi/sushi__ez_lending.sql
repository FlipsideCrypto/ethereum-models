{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id',
  cluster_by = ['block_timestamp::DATE'],
  enabled = false,
  meta={
      'database_tags':{
          'table': {
              'PROTOCOL': 'SUSHI',
              'PURPOSE': 'DEFI, DEX'
          }
      }
  }
) }}

WITH lending_txns AS (

  SELECT
    DISTINCT tx_hash,
    contract_address
  FROM
    {{ ref('silver__logs') }}
  WHERE
    topics [0] :: STRING = '0x30a8c4f9ab5af7e1309ca87c32377d1a83366c5990472dbf9d262450eae14e38'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
unlending_txns AS (
  SELECT
    DISTINCT tx_hash,
    contract_address
  FROM
    {{ ref('silver__logs') }}
  WHERE
    topics [0] :: STRING = '0x6e853a5fd6b51d773691f542ebac8513c9992a51380d4c342031056a64114228'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
lending AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    'Deposit' AS action,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    event_index,
    CONCAT ('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
    CONCAT ('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS lending_pool_address,
    origin_from_address AS lender,
    CONCAT ('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS lender2,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS amount,
      CASE
        WHEN lender = lender2 THEN 'no'
        ELSE 'yes'
      END AS lender_is_a_contract,
      _inserted_timestamp,
      _log_id
      FROM
        {{ ref('silver__logs') }}
      WHERE
        topics [0] :: STRING = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'
        AND tx_hash IN (
          SELECT
            tx_hash
          FROM
            lending_txns
        )
        AND CONCAT ('0x', SUBSTR(topics [3] :: STRING, 27, 40)) IN (
          SELECT
            address
          FROM
            {{ ref('silver__contracts') }}
          WHERE
            NAME ILIKE 'kashi Medium Risk%'
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
withdraw AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    'Withdraw' AS action,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    event_index,
    CONCAT ('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
    CONCAT ('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS lending_pool_address,
    origin_from_address AS lender,
    CONCAT ('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS lender2,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS amount,
      CASE
        WHEN lender = lender2 THEN 'no'
        ELSE 'yes'
      END AS lender_is_a_contract,
      _inserted_timestamp,
      _log_id
      FROM
        {{ ref('silver__logs') }}
      WHERE
        topics [0] :: STRING = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'
        AND tx_hash IN (
          SELECT
            tx_hash
          FROM
            unlending_txns
        )
        AND CONCAT ('0x', SUBSTR(topics [2] :: STRING, 27, 40)) IN (
          SELECT
            address
          FROM
            {{ ref('silver__contracts') }}
          WHERE
            NAME ILIKE 'kashi Medium Risk%'
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
total AS (
  SELECT
    *
  FROM
    lending
  UNION ALL
  SELECT
    *
  FROM
    withdraw
),
token_price AS (
  SELECT
    HOUR,
    token_address,
    price
  FROM
    {{ ref('core__fact_hourly_token_prices') }}
  WHERE
    1 = 1

{% if is_incremental() %}
AND HOUR :: DATE IN (
  SELECT
    DISTINCT block_timestamp :: DATE
  FROM
    lending
)
{% endif %}
),
labels AS (
  SELECT
    address,
    symbol,
    decimals
  FROM
    {{ ref('silver__contracts') }}
)
SELECT
  A.block_timestamp,
  A.block_number,
  A.tx_hash,
  A.action,
  A.origin_from_address,
  A.origin_to_address,
  A.origin_function_signature,
  A.asset,
  A.lender2 AS depositor,
  A.lender_is_a_contract,
  A.lending_pool_address,
  A.event_index,
  CASE
    WHEN d.decimals IS NULL THEN A.amount
    ELSE (A.amount / pow(10, d.decimals))
  END AS amount,
  (
    A.amount * C.price
  ) / pow(
    10,
    d.decimals
  ) AS amount_USD,
  b.symbol AS lending_pool,
  d.symbol AS symbol,
  A._inserted_timestamp,
  A._log_id
FROM
  total A
  LEFT JOIN token_price C
  ON DATE_TRUNC(
    'hour',
    A.block_timestamp
  ) = DATE_TRUNC(
    'hour',
    C.hour
  )
  AND A.asset = C.token_address
  LEFT JOIN labels b
  ON A.lending_pool_address = b.address
  LEFT JOIN labels d
  ON A.asset = d.address
