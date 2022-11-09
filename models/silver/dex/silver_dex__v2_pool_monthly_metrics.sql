{{ config(
  materialized = 'incremental',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = ['date','block_number','contract_address','pid'],
  cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH V1_allocpoint_per_pool AS (

  SELECT
   PUBLIC.udf_hex_to_int(function_input :: STRING)  as pid,
    CASE
      WHEN function_signature = '0x1526fe27' THEN CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40))
      ELSE contract_address
    END AS contract_address,
        Case 
    WHEN function_signature = '0x1526fe27' THEN  PUBLIC.udf_hex_to_int(segmented_data [1] :: STRING) 
    else PUBLIC.udf_hex_to_int(segmented_data [0] :: STRING)
    end as allocation_points,
    A.block_number,
    function_signature,
    segmented_data,
    call_name,
    b.block_timestamp :: DATE AS DATE,
    A._inserted_timestamp
  FROM
    {{ ref('bronze__successful_reads') }} A
    JOIN {{ ref('silver__blocks') }} b
    ON A.block_number = b.block_number
  WHERE read_output :: STRING <> '0x'
    and contract_address = '0xc2edad668740f1aa35e4d8f227fb8e17dca888cd' --Sushiswap Masterchef
    and function_signature in ('0x1526fe27','0x17caf6f1')

{% if is_incremental() %}
AND A._inserted_timestamp >= (
  SELECT
    MAX(
      _inserted_timestamp
    ) :: DATE
  FROM
    {{ this }}
)
{% endif %}
),


V2_PoolID_per_pool as (
  SELECT
     CASE
      WHEN function_signature = '0x78ed5d1f' THEN CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40))
      ELSE contract_address
    END AS contract_address,
    Case 
    WHEN function_signature = '0x17caf6f1' THEN  PUBLIC.udf_hex_to_int(segmented_data [0] :: STRING) 
    end as allocation_points,
    A.block_number,
    function_signature,
    segmented_data,
    function_input,
    call_name,
    b.block_timestamp :: DATE AS DATE,
    A._inserted_timestamp
  FROM
    {{ ref('bronze__successful_reads') }} A
    JOIN {{ ref('silver__blocks') }} b
    ON A.block_number = b.block_number
  WHERE read_output :: STRING <> '0x'
    and contract_address = '0xef0881ec094552b2e128cf945ef17a6752b4ec5d' --Sushiswap MasterchefV2
    and function_signature in ('0x78ed5d1f','0x17caf6f1')

{% if is_incremental() %}
AND A._inserted_timestamp >= (
  SELECT
    MAX(
      _inserted_timestamp
    ) :: DATE 
  FROM
    {{ this }}
)
{% endif %}

),

V2_allocpoint_per_poolID AS (
  SELECT
    a.block_number,
    function_input,
    call_name,
    function_signature,
    PUBLIC.udf_hex_to_int(
      segmented_data [2] :: STRING
    ) AS allocation_points
  FROM
    {{ ref('bronze__successful_reads') }} A
    JOIN {{ ref('silver__blocks') }} b
    ON A.block_number = b.block_number
  WHERE read_output :: STRING <> '0x'
    and contract_address = '0xef0881ec094552b2e128cf945ef17a6752b4ec5d' --Sushiswap MasterchefV2
    and function_signature = '0x1526fe27'

{% if is_incremental() %}
AND A._inserted_timestamp >= (
  SELECT
    MAX(
      _inserted_timestamp
    ) :: DATE
  FROM
    {{ this }}
)
{% endif %}
),


V2_allocpoint_per_pool as (
select 
A.Date as date,
PUBLIC.udf_hex_to_int(A.function_input :: STRING)  as pid,
A.contract_address,
A.block_number,
A._inserted_timestamp,
case when A.allocation_points is null then b.allocation_points else a.allocation_points
End as allocation_points,
case when A.allocation_points is null then b.call_name else a.call_name
End as call_name,
case when A.allocation_points is null then b.function_signature else a.function_signature
End as function_signature
from V2_PoolID_per_pool A
left join V2_allocpoint_per_poolID b
on a.block_number = b.block_number
and a.function_input = b.function_input
)
 SELECT
    Date,
    block_number,
    pid,
    contract_address,
    allocation_points,
    call_name,
    function_signature,
    _inserted_timestamp 
    FROM V1_allocpoint_per_pool
    union
     SELECT
    Date,
    block_number,
    pid,
    contract_address,
    allocation_points,
    call_name,
    function_signature,
    _inserted_timestamp 
    FROM V2_allocpoint_per_pool