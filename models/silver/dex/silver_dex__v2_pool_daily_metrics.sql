{{ config(
    materialized = 'incremental',
    persist_docs ={ "relation": true,
    "columns": true},
    unique_key = 'id',
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
        call_name in ('Balance_of_SLP_staked','Total_SLP_issued')

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

Balance_of_SLP_Staked as (
select
  block_number,
  contract_address,
  PUBLIC.udf_hex_to_int(segmented_data [0] :: STRING ) AS Balance_of_SLP_Staked
  from reads
  where function_signature = '0x70a08231' 
),

total_supply_of_SLP as (
select
  block_number,
  contract_address,
  PUBLIC.udf_hex_to_int(segmented_data [0] :: STRING ) AS total_supply_of_SLP
  from reads
  where function_signature = '0x18160ddd' 
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
 d.Balance_of_SLP_Staked,
 e.total_supply_of_SLP,
 a._inserted_timestamp as _inserted_timestamp,
  {{ dbt_utils.surrogate_key(
        ['a.block_number', 'a.contract_address', 'a.date']
    ) }} AS id
 from base a 
left join  Balance_of_SLP_Staked d
on a.block_number = d.block_number and a.contract_address = d.contract_address
left join total_supply_of_SLP e
on a.block_number = e.block_number and a.contract_address = e.contract_address
)

select 
* from
Final