{{ config(
  materialized = 'incremental',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = ['date','block_number','contract_address'],
  cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH reads AS (

  SELECT
    CASE
      WHEN function_signature = '0x1526fe27' THEN CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40))
      ELSE contract_address
    END AS contract_address,
    A.block_number,
    function_signature,
    segmented_data,
    b.block_timestamp :: DATE AS DATE,
    A._inserted_timestamp
  FROM
    {{ ref('bronze__successful_reads') }} A
    JOIN {{ ref('silver__blocks') }} b
    ON A.block_number = b.block_number
  WHERE
    call_name IN (
      'Total_alloc_points',
      'Pool_info_alloc_points'
    )
    AND read_output :: STRING <> '0x'

{% if is_incremental() %}
AND A._inserted_timestamp >= (
  SELECT
    MAX(
      _inserted_timestamp
    ) :: DATE - 1
  FROM
    {{ this }}
)
{% endif %}
),
allocation_points AS (
  SELECT
    block_number,
    contract_address,
    PUBLIC.udf_hex_to_int(
      segmented_data [1] :: STRING
    ) AS allocation_points
  FROM
    reads
  WHERE
    function_signature = '0x1526fe27'
),
total_allocation_points AS (
  SELECT
    block_number,
    contract_address,
    PUBLIC.udf_hex_to_int(
      segmented_data [0] :: STRING
    ) AS total_allocation_points
  FROM
    reads
  WHERE
    function_signature = '0x17caf6f1'
),
base AS (
  SELECT
    DISTINCT DATE,
    block_number,
    contract_address,
    _inserted_timestamp
  FROM
    reads
),
FINAL AS (
  SELECT
    A.date,
    A.block_number,
    A.contract_address,
    b.allocation_points,
    C.total_allocation_points,
    A._inserted_timestamp AS _inserted_timestamp
  FROM
    base A
    LEFT JOIN allocation_points b
    ON A.block_number = b.block_number
    AND A.contract_address = b.contract_address
    LEFT JOIN total_allocation_points C
    ON A.block_number = C.block_number
    AND A.contract_address = C.contract_address
)
SELECT
  *
FROM
  FINAL
