{{ config(
    materialized = 'incremental',
    unique_key = 'log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

With pool_name AS (
    SELECT pool_name,poolId, pool_address
    FROM 
        {{ref('silver_dex__balancer_pools')}}
)

,swaps_base as (
    SELECT 
        tx_hash,
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        _inserted_timestamp,
        event_name,
        event_index,
        event_inputs :amountIn :: INTEGER AS amountIn,
        event_inputs :amountOut :: INTEGER AS amountOut,
        event_inputs :poolId :: STRING AS poolId,
        event_inputs :tokenIn :: STRING As token_in,
        event_inputs :tokenOut ::STRING AS token_out,
        SUBSTR(
            event_inputs :poolId :: STRING, 0, 42 ) AS pool_address,
        _log_id,
        ingested_at,
        'balancer'AS platform,
        origin_from_address as sender,
        origin_from_address AS tx_to 
    From 
        {{ref('silver__logs')}}
    WHERE 
         contract_address = lower('0xBA12222222228d8Ba445958a75a0704d566BF2C8')
    And event_name = 'Swap'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}

)
,contracts AS (
    SELECT *
    From 
        {{ref('core__dim_contracts')}}
    WHERE decimals is not null 
    AND (address in (
        select distinct token_in
        from swaps_base
        )
    OR address IN (
        SELECT distinct token_out
        From swaps_base))


)
,hourly_token_price AS (
   SELECT 
         HOUR,
         token_address,
         AVG(price) as price
    FROM 
        {{ref('core__fact_hourly_token_prices')}}

    WHERE token_address in (
        SELECT DISTINCT address 
        From contracts
        )
    AND HOUR :: DATE in (select distinct block_timestamp::date from swaps_base)
    GROUP by 1, 2
    
)
select 
        tx_hash,
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        _inserted_timestamp,
        S.event_name,
        event_index,
        amountIn as amountIn_unadj,
        c1.decimals as decimals_in,
        c1.symbol as symbol_in,
        case when decimals_in is null then amountIn_unadj else (amountIn_unadj / pow(10,decimals_in)) end as amount_in,
        case when decimals_in is not null then round(amount_in * p1.price,2) end as amount_in_usd,
        amountOut as amountOut_unadj,
        c2.decimals as decimals_out,
        c2.symbol as symbol_out,
        case when decimals_out is null then amountOut_unadj else (amountOut_unadj / pow(10,decimals_out)) end as amount_out,
        case when decimals_out is not null then round(amount_out * p2.price,2) end as amount_out_usd,
        pn.poolId,
        token_in,
        token_out,
        S.pool_address,
        S._log_id,
        S.ingested_at,
        S.platform,
        sender,
        tx_to,
        pool_name
from swaps_base S
left join contracts c1
on token_in = c1.address
left join contracts c2
on token_out = c2.address
left join hourly_token_price p1
on token_in = p1.token_address
and date_trunc('hour',block_timestamp) = p1.hour
left join hourly_token_price p2
on token_out = p2.token_address
and date_trunc('hour',block_timestamp) = p2.hour
Left Join pool_name pn 
ON pn.pool_address = S.pool_address
Where pool_name is not NULL