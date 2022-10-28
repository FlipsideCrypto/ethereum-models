{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    tags = ['streamline_view']
) }}

with Block_number as (
select block_timestamp::date as Day, min(block_number) as block_number
    from {{ ref('silver__blocks') }}
where Day >= '2020-02-01'
{% if is_incremental() %}
and _inserted_timestamp >=  (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
    group by 1
),

    
daily_price as (
select recorded_hour::date as Day, token_address, symbol, avg(open) daily_price from
  (select a.recorded_hour, a.open, b.token_address, b.symbol 
    from    
            {{ source(
            'crosschain_silver',
            'HOURLY_PRICES_COIN_GECKO') }} a join 
            {{ source(
            'crosschain_silver',
            'ASSET_METADATA_COIN_GECKO') }} b on a.ID = b.id
  where recorded_hour::date = current_date -1  and b.token_address is not null and b.platform = 'ethereum')
 group by 1,2,3
),

pools as ( 
 select distinct pool_address from {{ ref("core__dim_dex_liquidity_pools") }}  where platform in('sushiswap','uniswap-v2') ),
    
balance as (
select block_timestamp::date as Date, address, contract_address, min(balance) as balance 
from {{ ref("silver__token_balances") }} 
where address in (select * from pools) and block_timestamp::date = current_date -1 
group by 1,2,3),

decimals as (
select address, decimals
    from {{ ref("core__dim_contracts") }}  
    ),

Pools_value as (
select c.Date, c.address, c.contract_address, d.Day , d.symbol, c.balance * d.daily_price / power(10,e.decimals) as value_USD
from balance c
left join daily_price d
on c.Date = d.Day and c.contract_address = d.token_address
left join decimals e
on c.contract_address = e.address
),

Top_pools as (
select Date, address, sum(value_usd) as pool_value
    from Pools_value
    group by 1,2
Qualify row_number() over(order by zeroifnull(pool_value) desc) <= 200    
),


balance_of_slp_reads as (
    SELECT
        Day,
         block_number,
         address as contract_address,
         '0xc2edad668740f1aa35e4d8f227fb8e17dca888cd' as function_input,
        '0x70a08231' AS function_signature
    FROM Block_number 
    join Top_pools

),


Total_SLP_supply_reads as (
 select
  Day,
  block_number,
  address as contract_address,
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
        balance_of_slp_reads
    UNION ALL
    SELECT
        Day,
        block_number,
        contract_address,
        'Total_SLP_issued' call_name,
        function_input,
        function_signature
    FROM
        Total_SLP_supply_reads

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
