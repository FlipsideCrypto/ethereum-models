{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id',
  cluster_by = ['block_timestamp::DATE']
) }}

with lending_txns as (
select distinct tx_hash,contract_address
from ETHEREUM.CORE.FACT_EVENT_LOGS
where event_name = 'LogAddAsset'
),

unlending_txns as (
select distinct tx_hash,contract_address
from ETHEREUM.CORE.FACT_EVENT_LOGS
where event_name = 'LogRemoveAsset'
),

token_price as (
select hour, token_address, symbol, price, decimals
from  ETHEREUM.CORE.FACT_HOURLY_TOKEN_PRICES
where 1=1 
{% if is_incremental() %}
AND hour::DATE >= (
  SELECT
    MAX(block_timestamp::DATE)
  FROM
    {{ this }}
)
{% endif %}


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
        case when Lender = Lender2 then 'no' 
        else 'yes' end as Lender_is_a_contract,
        _log_id
from {{ ref('core__fact_event_logs') }}
where event_name = 'LogTransfer' and tx_hash in (select tx_hash from lending_txns)
and event_inputs:to::string in (select distinct contract_address from lending_txns )
{% if is_incremental() %}
AND block_timestamp::DATE >= (
  SELECT
    MAX(block_timestamp::DATE)
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
        event_inputs:share::number as amount,
        case when Lender = Lender2 then 'no' 
        else 'yes' end as Lender_is_a_contract,
        _log_id
from {{ ref('core__fact_event_logs') }}
where event_name = 'LogTransfer' and tx_hash in (select tx_hash from unlending_txns)
and event_inputs:from::string in (select distinct contract_address from unlending_txns) 
{% if is_incremental() %}
AND block_timestamp::DATE >= (
  SELECT
    MAX(block_timestamp::DATE)
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
a.tx_hash,
a.action,
a.asset,
a.Lender2 as lender,
a.lender_is_a_contract,
a.lending_pool_address,
a.event_index,
(a.amount/pow(10,c.decimals)) as amount,
(a.amount* c.price)/pow(10,c.decimals) as amount_USD,
b.symbol as lending_pool,
c.symbol as symbol,
a._log_id
from FINAL a
left join token_price c 
on date_trunc('hour',a.block_timestamp) = c.hour and a.asset = c.token_address
left join labels b 
on a.Lending_pool_address = b.address



