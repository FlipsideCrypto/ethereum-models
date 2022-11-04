{{ config(
    materialized = 'incremental',
    persist_docs ={ "relation": true,
    "columns": true },
    unique_key = ['date','block_number','contract_address'],
    cluster_by = ['_inserted_timestamp::DATE'] 
) }}

with reads as (
 select 
 case  when function_signature = '0x1526fe27' 
        then CONCAT('0x',SUBSTR(segmented_data[0]::string, 25, 40)) 
        else contract_address 
        end as contract_address,
  block_number,
  function_signature,
  segmented_data,
  _inserted_timestamp from 
   {{ref('bronze__successful_reads')}}
where    
        call_name in ('Total_alloc_points','Pool_info_alloc_points')

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
Block_date as (
    SELECT block_timestamp::date as date, block_number, _inserted_timestamp 
    from {{ref('silver__blocks')}} 
    where date >= '2020-02-01'
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

contracts as (
  select distinct block_number, contract_address
  from reads
),

allocation_points as (
select
  block_number,
  contract_address,
  PUBLIC.udf_hex_to_int(segmented_data [1] :: STRING ) AS allocation_points
  from reads
  where function_signature = '0x1526fe27' 
),

total_allocation_points as (
select
  block_number,
  contract_address,
  PUBLIC.udf_hex_to_int(segmented_data [0] :: STRING ) AS total_allocation_points
  from reads
  where function_signature = '0x17caf6f1' 
),

base as (
select 
 a.date, 
 a.block_number, 
 b.contract_address,
 a._inserted_timestamp
from Block_date a
right join contracts b
on a.block_number = b.block_number
),


Final as (
select 
 a.date,
 a.block_number,
 a.contract_address,
 b.allocation_points,
 c.total_allocation_points,
 a._inserted_timestamp as _inserted_timestamp
 from base a 
 left join allocation_points b
 on a.block_number = b.block_number and a.contract_address = b.contract_address
 left join total_allocation_points c 
on a.block_number = c.block_number and a.contract_address = c.contract_address
)

select 
* from
Final