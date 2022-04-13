{{ config(
  materialized = 'incremental',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id',
  cluster_by = ['ingested_at::DATE']
) }}
 
With 
swap_events as ( 
  select  block_number,
          block_timestamp,
          tx_hash,
          contract_address,
          event_name,
          event_inputs:amount0In::int as amount0In,
          event_inputs:amount1In::int as amount1In,
          event_inputs:amount0Out::int as amount0Out,
          event_inputs:amount1Out::int as amount1Out,
          event_inputs:sender::string as sender,
          event_inputs:to::string as Swap_initiator,
          event_index,
          _log_id,
          ingested_at
    from  {{ref('silver__logs')}}
    where event_name = 'Swap'
          {% if is_incremental() %}
      AND ingested_at >= (select MAX(ingested_at) FROM {{ this }})
          {% endif %}
          ),
  
token_info as (
  select a.address as address,
         a.name,
         case when a.name ilike '%Sushi%' then 'Sushiswap'
              when a.name ilike '%Uni%'  then  'Uniswap'
         end as platform, 
         a.contract_metadata:token0::string as token0, 
         a.contract_metadata:token1::string as token1, 
         b.symbol as symbol0, 
         c.symbol as symbol1, 
         b.decimals as decimal0, 
         c.decimals as decimal1 
     from {{ref('core__dim_contracts')}} a
     left join {{ref('core__dim_contracts')}} b
     on lower(a.contract_metadata:token0) = b.address
     left join {{ref('core__dim_contracts')}} c
     on lower(a.contract_metadata:token1) = c.address), 
 
 prices as (
 select token_address,
        hour,
        symbol,
        avg(price) as price  
   from {{ref('core__fact_hourly_token_prices')}}
  where 1 = 1
        {% if is_incremental() %}
   and  hour::date >= (select MAX(block_timestamp::date) FROM {{ this }})
        {% endif %}
group by token_address,
         hour,
         symbol), 

swap_without_price as (
  select
          se.block_number as block_number ,
          se.block_timestamp as block_timestamp,
          se.tx_hash as tx_hash,
          se.contract_address as contract_Address,
          se.event_name as event_name,
          CASE 
          WHEN se.amount0In <> 0 THEN se.amount0In / power(10, decimal0)::float
          WHEN se.amount1In <> 0 THEN se.amount1In/ power(10, decimal1)::float
          END AS amount_in,
          CASE 
          WHEN se.amount0Out <> 0 THEN se.amount0Out/ power(10, decimal0)::float
          WHEN se.amount1Out <> 0 THEN se.amount1Out/ power(10, decimal1)::float
          END as amount_out,
          se.sender as sender,
          se.Swap_initiator as Swap_initiator,
          se.event_index as event_index,
          se._log_id,
          token_info.name as contract_name,
          token_info.platform as platform,
          se.ingested_at,
          CASE 
          WHEN se.amount0In <> 0 THEN token0
          WHEN se.amount1In <> 0 THEN token1
          END AS token_In,
          CASE 
          WHEN se.amount0Out <> 0 THEN token0
          WHEN se.amount1Out <> 0 THEN token1
          END AS token_out,
          CASE 
          WHEN se.amount0In <> 0 THEN symbol0
          WHEN se.amount1In <> 0 THEN symbol1
          END AS symbol_In,
          CASE 
          WHEN se.amount0Out <> 0 THEN symbol0
          WHEN se.amount1Out <> 0 THEN symbol1
          END AS symbol_out  
    from  swap_events se
left join token_info
      on  se.contract_address = token_info.address)
      
      
  select 
          wp.block_number ,
          wp.block_timestamp,
          wp.tx_hash,
          wp.contract_Address,
          wp.event_name,
          wp.amount_in,
          wp.amount_out,
          wp.sender,
          wp.Swap_initiator,
          wp.event_index,
          wp._log_id,
          wp.contract_name,
          wp.platform,
          wp.token_in,
          wp.token_out,
          wp.symbol_in,
          wp.symbol_out,
          wp.ingested_at,
          wp.amount_in * pIn.price as amount_in_usd,
          wp.amount_out * pOut.price as amount_out_usd    
  from    swap_without_price wp
left join prices pIn
    on    lower(wp.token_in) = lower(pIn.token_Address)
    and   date_trunc('hour',wp.block_timestamp) = pIn.hour
left join prices pOut
    on    lower(wp.token_out) = lower(pOut.token_address)
    and   date_trunc('hour',wp.block_timestamp) = pOut.hour
    WHERE wp.platform ilike '%sushi%'
