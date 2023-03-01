{{ config(
  materialized = 'incremental',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = ['date','block_number','contract_address','pid'],
  cluster_by = ['_inserted_timestamp::DATE'],
  enabled = false
) }}

WITH block_date AS (

  SELECT
    block_timestamp :: DATE AS DATE,
    block_number,
    _inserted_timestamp
  FROM
    {{ ref('silver__blocks') }}
  WHERE
    DATE >= '2020-09-01'

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(
      _inserted_timestamp
    ) :: DATE
  FROM
    {{ this }}
)
{% endif %}
),
v1_allocpoint_per_pool AS (
  SELECT
    A.contract_address,
    CASE
      WHEN function_signature = '0x1526fe27' THEN CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40))
      ELSE contract_address
    END AS pool_address,
    CASE
      WHEN function_signature = '0x1526fe27' THEN PUBLIC.udf_hex_to_int(
        segmented_data [1] :: STRING
      )
      ELSE PUBLIC.udf_hex_to_int(
        segmented_data [0] :: STRING
      )
    END AS allocation_points,
    A.block_number,
    function_signature,
    PUBLIC.udf_hex_to_int(
      A.function_input :: STRING
    ) AS pid,
    CASE
      WHEN function_signature = '0x1526fe27' THEN 'poolInfo'
      ELSE 'totalAllocPoint'
    END AS function_name,
    call_name,
    b.date,
    A._inserted_timestamp
  FROM
    {{ ref('bronze__successful_reads') }} A
    JOIN block_date b
    ON A.block_number = b.block_number
  WHERE
    read_output :: STRING <> '0x'
    AND contract_address = '0xc2edad668740f1aa35e4d8f227fb8e17dca888cd' --Sushiswap Masterchef
    AND function_signature IN (
      '0x1526fe27',
      '0x17caf6f1'
    )
),
v2_poolid_per_pool AS (
  SELECT
    A.contract_Address,
    CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS pool_address,
    A.block_number,
    function_signature,
    PUBLIC.udf_hex_to_int(
      A.function_input :: STRING
    ) AS pid,
    function_input,
    'lpToken' AS function_name,
    call_name,
    b.date,
    A._inserted_timestamp
  FROM
    {{ ref('bronze__successful_reads') }} A
    JOIN block_date b
    ON A.block_number = b.block_number
  WHERE
    read_output :: STRING <> '0x'
    AND contract_address = '0xef0881ec094552b2e128cf945ef17a6752b4ec5d' --Sushiswap MasterchefV2
    AND function_signature = '0x78ed5d1f'
),
v2_allocpoint_per_poolid AS (
  SELECT
    A.contract_address,
    A.block_number,
    PUBLIC.udf_hex_to_int(
      A.function_input :: STRING
    ) AS pid,
    function_input,
    call_name,
    'poolInfo' AS function_name,
    function_signature,
    A._inserted_timestamp,
    PUBLIC.udf_hex_to_int(
      segmented_data [2] :: STRING
    ) AS allocation_points
  FROM
    {{ ref('bronze__successful_reads') }} A
    JOIN block_date b
    ON A.block_number = b.block_number
  WHERE
    read_output :: STRING <> '0x'
    AND contract_address = '0xef0881ec094552b2e128cf945ef17a6752b4ec5d' --Sushiswap MasterchefV2
    AND function_signature = '0x1526fe27'
),
v2_allocpoint_per_pool AS (
  SELECT
    A.date,
    A.pid,
    A.contract_address,
    A.pool_address,
    A.block_number,
    A._inserted_timestamp,
    b.allocation_points,
    b.call_name,
    b.function_name,
    b.function_signature
  FROM
    v2_poolid_per_pool A
    LEFT JOIN v2_allocpoint_per_poolid b
    ON A.block_number = b.block_number
    AND A.function_input = b.function_input
    AND A.contract_address = b.contract_address
)
SELECT
  DATE,
  block_number,
  pid,
  contract_address,
  pool_address,
  allocation_points,
  function_name,
  function_signature,
  _inserted_timestamp
FROM
  v1_allocpoint_per_pool
UNION ALL
SELECT
  DATE,
  block_number,
  pid,
  contract_address,
  pool_address,
  allocation_points,
  function_name,
  function_signature,
  _inserted_timestamp
FROM
  v2_allocpoint_per_pool
