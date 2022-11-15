{{ config(
  materialized = 'incremental',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = ['date','block_number','contract_address','pid'],
  cluster_by = ['_inserted_timestamp::DATE']
) }}


WITH V1_allocpoint_per_pool AS (

  SELECT
    a.contract_address,
    CASE
      WHEN function_signature = '0x1526fe27' THEN CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40))
      ELSE contract_address
    END AS pool_address,
        Case 
    WHEN function_signature = '0x1526fe27' THEN  PUBLIC.udf_hex_to_int(segmented_data [1] :: STRING) 
    else PUBLIC.udf_hex_to_int(segmented_data [0] :: STRING)
    end as allocation_points,
    A.block_number,
    function_signature,
    PUBLIC.udf_hex_to_int(A.function_input :: STRING)  as pid,
    case when function_signature = '0x1526fe27' then 'poolInfo' else 'totalAllocPoint' end as function_name,
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
    a.contract_Address,
    CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) As pool_address,
    A.block_number,
    function_signature,
    PUBLIC.udf_hex_to_int(A.function_input :: STRING)  as pid,
    'lpToken' as function_name,
    call_name,
    b.block_timestamp :: DATE AS DATE,
    A._inserted_timestamp
  FROM
  {{ ref('bronze__successful_reads') }} A
    JOIN {{ ref('silver__blocks') }} b
    ON A.block_number = b.block_number
  WHERE read_output :: STRING <> '0x'
    and contract_address = '0xef0881ec094552b2e128cf945ef17a6752b4ec5d' --Sushiswap MasterchefV2
    and function_signature = '0x78ed5d1f'
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
    a.contract_address,
    a.block_number,
    PUBLIC.udf_hex_to_int(A.function_input :: STRING)  as pid,
    call_name,
    'poolInfo' as function_name,
    function_signature,
    A._inserted_timestamp,
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
A.Date,
A.pid,
A.contract_address,
A.pool_address,
A.block_number,
b.allocation_points,
b.call_name,
b.function_name,
b.function_signature,
a._inserted_timestamp
from V2_PoolID_per_pool A
left join V2_allocpoint_per_poolID b
on a.block_number = b.block_number
and a.pid = b.pid
and a.contract_address = b.contract_address
)

  SELECT 
    Date,
    block_number,
    pid, 
    contract_address,
    pool_address,
    allocation_points,
    function_name,
    function_signature,
    _inserted_timestamp
    from V1_allocpoint_per_pool 
    union all
    select
    Date,
    block_number,
    pid,
    contract_address,
    pool_address,
    allocation_points,
    function_name,
    function_signature,
    _inserted_timestamp
    from V2_allocpoint_per_pool
