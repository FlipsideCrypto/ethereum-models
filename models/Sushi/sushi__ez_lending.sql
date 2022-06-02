{{ config(
  materialized = 'incremental',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id',
  cluster_by = ['ingested_at::DATE']
) }}

with lending_txns as (
select distinct tx_hash
from ETHEREUM.CORE.FACT_EVENT_LOGS
where event_name = 'LogAddAsset'
),

unlending_txns as (
select distinct tx_hash
from ETHEREUM.CORE.FACT_EVENT_LOGS
where event_name = 'LogRemoveAsset'
),

token_price as (
select hour, token_address, symbol, price, decimals
from  ETHEREUM.CORE.FACT_HOURLY_TOKEN_PRICES
),

labels as (
select address, symbol
from ETHEREUM.CORE.DIM_CONTRACTS
),


Lending as (
select  block_timestamp,
        block_number,
        tx_hash, 
        'Lending' as action, 
        event_index,
        event_inputs:token::string as asset, 
        event_inputs:to::string as Lending_pool_address, 
        origin_from_address as Lender, 
        event_inputs:from::string as Lender2, 
        event_inputs:share::number as amount,
        ingested_at,
        _log_id
from {{ ref('core__fact_event_logs') }}
where event_name = 'LogTransfer' and tx_hash in (select * from lending_txns)
{% if is_incremental() %}
AND ingested_at >= (
  SELECT
    MAX(ingested_at)
  FROM
    {{ this }}
)
{% endif %}

),

Withdraw as (
select  block_timestamp, 
        block_number,
        tx_hash, 
        'Withdraw' as action, 
        event_index,
        event_inputs:token::string as asset, 
        event_inputs:from::string as Lending_pool_address, 
        origin_from_address as Lender, 
        event_inputs:to::string as Lender2, 
        event_inputs:share::number as amount
        ingested_at,
        _log_id
from {{ ref('core__fact_event_logs') }}
where event_name = 'LogTransfer' and tx_hash in (select * from lending_txns)
{% if is_incremental() %}
AND ingested_at >= (
  SELECT
    MAX(ingested_at)
  FROM
    {{ this }}
)
{% endif %}
),

Final as (
select * from Lending
union all
select * from Withdraw
)

select 
a.block_timestamp,
a.block_number,
a.block_n
a.tx_hash,
a.action,
a.Asset,
a.Lender,
a.Lender2,
a.Lending_pool_address,
a.event_index,
(a.amount/pow(10,c.decimals)) as amount,
(a.amount* c.price)/pow(10,c.decimals) as amount_USD,
b.symbol as Lending_pool,
c.symbol as symbol,
a._log_id
from FINAL a
left join token_price c 
on date_trunc('hour',a.block_timestamp) = c.hour and a.asset = c.token_address
left join labels b 
on a.Lending_pool_address = b.address



