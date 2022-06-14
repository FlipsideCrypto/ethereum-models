{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id',
  cluster_by = ['block_timestamp::DATE']
) }}

with borrow_txn as (
select distinct tx_hash,contract_address
from {{ ref('silver__logs') }}
where event_name = 'LogBorrow'
{% if is_incremental() %}
AND ingested_at::DATE >= (
  SELECT
    MAX(ingested_at) ::DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),

Repay_txns as (
select distinct tx_hash,contract_address
from {{ ref('silver__logs') }}
where event_name = 'LogRepay'
{% if is_incremental() %}
AND ingested_at::DATE >= (
  SELECT
    MAX(ingested_at) ::DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),


Borrow as (
select  block_timestamp,
        block_number,
        tx_hash, 
        'Borrow' as action, 
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        event_index,
        event_inputs:token::string as asset,
        event_inputs:from::string as Lending_pool_address, 
        origin_from_address as Borrower, 
        event_inputs:to::string as Borrower2,  
        event_inputs:share::number as amount,
        case when Borrower = Borrower2 then 'no' 
        else 'yes' end as Borrower_is_a_contract,
        ingested_at,
        _log_id
from {{ ref('silver__logs') }}
where event_name = 'LogTransfer' and tx_hash in (select tx_hash from borrow_txn)
and event_inputs:from::string in (select ADDRESS from {{ ref('silver__contracts') }} where name ilike 'kashi Medium Risk%' )
{% if is_incremental() %}
AND ingested_at::DATE >= (
  SELECT
    MAX(ingested_at) ::DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),


Repay as (
select  block_timestamp, 
        block_number,
        tx_hash, 
        'Repay' as action,
        origin_from_address,
        origin_to_address,
        origin_function_signature, 
        event_index,
        event_inputs:token::string as asset, 
        event_inputs:to::string as Lending_pool_address, 
        origin_from_address as Borrower, 
        event_inputs:from::string as Borrower2, 
        event_inputs:share::number as amount,
        case when Borrower = Borrower2 then 'no' 
        else 'yes' end as Borrower_is_a_contract,
        ingested_at,
        _log_id
from {{ ref('silver__logs') }}
where event_name = 'LogTransfer' and tx_hash in (select tx_hash from Repay_txns)
and event_inputs:to::string in (select ADDRESS from {{ ref('silver__contracts') }} where name ilike 'kashi Medium Risk%') 
{% if is_incremental() %}
AND ingested_at::DATE >= (
  SELECT
    MAX(ingested_at) ::DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),


FINAL as (
select * from Borrow
union all
select * from Repay
),



token_price as (
select hour, token_address, price 
from {{ ref('core__fact_hourly_token_prices') }}
where 1=1 
{% if is_incremental() %}
AND HOUR :: DATE IN (
    SELECT
        DISTINCT block_timestamp :: DATE
    FROM
        Borrow
)
{% endif %}
),

labels as (
select address, symbol, decimals
from {{ ref('silver__contracts') }}
)

select 
a.block_timestamp,
a.block_number,
a.tx_hash,
a.action,
a.origin_from_address,
a.origin_to_address,
a.origin_function_signature,
a.event_index,
a.asset,
a.lending_pool_address,
a.Borrower2 as Borrower,
a.Borrower_is_a_contract,
a.ingested_at,
a._log_id,
case when d.decimals is null then a.amount else (a.amount/pow(10,d.decimals)) end as amount,
(a.amount* c.price)/pow(10,d.decimals) as amount_USD,
b.symbol as lending_pool,
d.symbol as symbol,
substring(b.symbol,3,charindex('/',b.symbol)-3) as collateral_symbol
from FINAL a
left join token_price c 
on date_trunc('hour',a.block_timestamp) = date_trunc('hour',c.hour)  and a.asset = c.token_address
left join labels b 
on a.Lending_pool_address = b.address
left join labels d
on a.asset = d.address


