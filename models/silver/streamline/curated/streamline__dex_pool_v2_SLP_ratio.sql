{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    tags = ['streamline_view']
) }}

with Block_number as (
select block_timestamp::date as Day, min(block_number) as block_number
    from {{ ref('core__fact_blocks') }}
where Day >= '2020-02-01'
{% if is_incremental() %}
and Day >=  (
    SELECT
        MAX(
            Day
        )
    FROM
        {{ this }}
)
{% endif %}
    group by 1
),

pools as (
select distinct pool_address from ETHEREUM.CORE.DIM_DEX_LIQUIDITY_POOLS where platform in('sushiswap','uniswap-v2')
),

Top_pools as(
    select user_address, sum(USD_VALUE_NOW) as value
from {{ ref('core__ez_current_balances') }}
where user_address in (select * from pools) and last_recorded_price::date = current_Date and usd_value_now is not null
group by 1
Qualify row_number() over(order by value desc) <= 300
),

balance_of_slp as (
    SELECT
        Day,
         block_number,
         user_address as contract_address,
         '0xc2edad668740f1aa35e4d8f227fb8e17dca888cd' as function_input,
        '0x70a08231' AS function_signature
    FROM Block_number 
    join Top_pools

),


Total_SLP_supply as (
 select
  Day,
  block_number,
  user_address as contract_address,
  '' as function_input,
  '0x18160ddd' AS function_signature
    FROM Block_number 
    join Top_pools



),
FINAL AS (
    SELECT
        Day,
        block_number,
        contract_address,
        'Balance_of_SLP_staked' call_name,
        function_input,
        function_signature
    FROM
        balance_of_slp
    UNION ALL
    SELECT
        Day,
        block_number,
        contract_address,
        'Total_SLP_issued' call_name,
        function_input,
        function_signature
    FROM
        Total_SLP_supply

)
SELECT
    {{ dbt_utils.surrogate_key(
        ['block_number', 'contract_address', 'function_signature', 'function_input']
    ) }} AS id,
    Day,
    block_number,
    contract_address,
    call_name,
    function_signature,
    function_input
FROM
    FINAL 
