{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id',
  cluster_by = ['block_timestamp::DATE']
) }}

WITH borrow_txns AS (

  SELECT
    DISTINCT tx_hash,
    contract_address
  FROM
    {{ ref('silver__logs') }}
  WHERE
    topics [0] :: STRING = '0x3a5151e57d3bc9798e7853034ac52293d1a0e12a2b44725e75b03b21f86477a6'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
repay_txns AS (
  SELECT
    DISTINCT tx_hash,
    contract_address
  FROM
    {{ ref('silver__logs') }}
  WHERE
    topics [0] :: STRING = '0xc8e512d8f188ca059984b5853d2bf653da902696b8512785b182b2c813789a6e'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
add_asset AS (
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
remove_asset AS (
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
borrow AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    'Borrow' AS action,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    event_index,
    CONCAT ('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
    CONCAT ('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS lending_pool_address,
    origin_from_address AS borrower,
    CONCAT ('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS borrower2,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS amount,
      CASE
        WHEN borrower = borrower2 THEN 'no'
        ELSE 'yes'
      END AS borrower_is_a_contract,
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
            borrow_txns
        )
        AND CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) IN (
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
add_coll_same_txn AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    'add collateral' AS action,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    event_index,
    CONCAT ('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
    CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS lending_pool_address,
    origin_from_address AS borrower,
    CONCAT ('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS borrower2,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS amount,
      CASE
        WHEN borrower = borrower2 THEN 'no'
        ELSE 'yes'
      END AS borrower_is_a_contract,
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
            borrow_txns
        )
        AND CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) IN (
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
repay AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    'Repay' AS action,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    event_index,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
    CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS lending_pool_address,
    origin_from_address AS borrower,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS borrower2,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS amount,
      CASE
        WHEN borrower = borrower2 THEN 'no'
        ELSE 'yes'
      END AS borrower_is_a_contract,
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
            repay_txns
        )
        AND CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) IN (
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
remove_coll_same_txn AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    'Remove collateral' AS action,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    event_index,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS lending_pool_address,
    origin_from_address AS borrower,
    CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS borrower2,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS amount,
      CASE
        WHEN borrower = borrower2 THEN 'no'
        ELSE 'yes'
      END AS borrower_is_a_contract,
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
            repay_txns
        )
        AND CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) IN (
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
add_coll_in_separate_txn AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    'add collateral' AS action,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    event_index,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
    CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS lending_pool_address,
    origin_from_address AS borrower,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS borrower2,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS amount,
      CASE
        WHEN borrower = borrower2 THEN 'no'
        ELSE 'yes'
      END AS borrower_is_a_contract,
      _inserted_timestamp,
      _log_id
      FROM
        ethereum_dev.silver.logs
      WHERE
        topics [0] :: STRING = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'
        AND tx_hash NOT IN (
          SELECT
            tx_hash
          FROM
            borrow_txns
        )
        AND tx_hash NOT IN (
          SELECT
            tx_hash
          FROM
            repay_txns
        )
        AND tx_hash NOT IN (
          SELECT
            tx_hash
          FROM
            add_asset
        )
        AND CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) IN (
          SELECT
            address
          FROM
            ethereum_dev.silver.contracts
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
remove_coll_in_separate_txn AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    'Remove collateral' AS action,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    event_index,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS lending_pool_address,
    origin_from_address AS borrower,
    CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS borrower2,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS amount,
      CASE
        WHEN borrower = borrower2 THEN 'no'
        ELSE 'yes'
      END AS borrower_is_a_contract,
      _inserted_timestamp,
      _log_id
      FROM
        ethereum_dev.silver.logs
      WHERE
        topics [0] :: STRING = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'
        AND tx_hash NOT IN (
          SELECT
            tx_hash
          FROM
            borrow_txns
        )
        AND tx_hash NOT IN (
          SELECT
            tx_hash
          FROM
            repay_txns
        )
        AND tx_hash NOT IN (
          SELECT
            tx_hash
          FROM
            remove_asset
        )
        AND CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) IN (
          SELECT
            address
          FROM
            ethereum_dev.silver.contracts
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
    borrow
  UNION ALL
  SELECT
    *
  FROM
    add_coll_same_txn
  UNION ALL
  SELECT
    *
  FROM
    remove_coll_same_txn
  UNION ALL
  SELECT
    *
  FROM
    repay
  UNION ALL
  SELECT
    *
  FROM
    add_coll_in_separate_txn
  UNION ALL
  SELECT
    *
  FROM
    remove_coll_in_separate_txn
),
token_price AS (
  SELECT
    HOUR,
    token_address,
    avg(price) as price
  FROM
    {{ ref('core__fact_hourly_token_prices') }}

{% if is_incremental() %}
where HOUR :: DATE IN (
  SELECT
    DISTINCT block_timestamp :: DATE
  FROM
    borrow
)
{% endif %}
group by 1,2
),
labels AS (
  SELECT
    address,
    symbol,
    decimals
  FROM
    {{ ref('silver__contracts') }}
),

Final as (
SELECT
  A.block_timestamp,
  A.block_number,
  A.tx_hash,
  A.action,
  A.origin_from_address,
  A.origin_to_address,
  A.origin_function_signature,
  A.borrower2 AS borrower,
  A.borrower_is_a_contract,
  A.lending_pool_address,
  b.symbol AS lending_pool,
  A.asset,
  d.symbol AS symbol,
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
  A._log_id,
  A._inserted_timestamp
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
  ON A.asset = d.address)


  select 
  * from 
  Final 
  where action = 'Borrow'
  and symbol <> substr(lending_pool,3,CHARINDEX('/',lending_pool)-3)
  union 
  select 
  * from 
  Final 
  where action = 'add collateral'
  and symbol = substr(lending_pool,3,CHARINDEX('/',lending_pool)-3)
   union 
  select 
  * from 
  Final 
  where action = 'Remove collateral'
  and symbol = substr(lending_pool,3,CHARINDEX('/',lending_pool)-3)
   union 
  select 
  * from 
  Final 
  where action = 'Repay'
  and symbol <> substr(lending_pool,3,CHARINDEX('/',lending_pool)-3)
