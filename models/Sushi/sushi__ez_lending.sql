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
from {{ ref('silver__logs') }}
where topics [0]::string = '0x30a8c4f9ab5af7e1309ca87c32377d1a83366c5990472dbf9d262450eae14e38'
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),

unlending_txns as (
select distinct tx_hash,contract_address
from {{ ref('silver__logs') }}
where topics [0]::string = '0x6e853a5fd6b51d773691f542ebac8513c9992a51380d4c342031056a64114228'
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),

Lending as (
select  block_timestamp,
        block_number,
        tx_hash, 
        'Deposit' as action, 
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        event_index,
        concat ('0x', SUBSTR(topics [1] :: STRING, 27, 40)) as asset, 
        concat ('0x', SUBSTR(topics [3] :: STRING, 27, 40)) as Lending_pool_address, 
        origin_from_address as Lender, 
        concat ('0x', SUBSTR(topics [2] :: STRING, 27, 40)) as Lender2, 
        TRY_TO_NUMBER(
            public.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA)))::integer
        ) as amount,
        case when Lender = Lender2 then 'no' 
        else 'yes' end as Lender_is_a_contract,
        _inserted_timestamp,
        _log_id
from {{ ref('silver__logs') }}
where topics [0]::string = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a' and tx_hash in (select tx_hash from lending_txns)
and concat ('0x', SUBSTR(topics [3] :: STRING, 27, 40)) in (select ADDRESS from {{ ref('silver__contracts') }} where name ilike 'kashi Medium Risk%' )
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
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
        origin_from_address,
        origin_to_address,
        origin_function_signature, 
        event_index,
        concat ('0x', SUBSTR(topics [1] :: STRING, 27, 40)) as asset, 
        concat ('0x', SUBSTR(topics [2] :: STRING, 27, 40)) as Lending_pool_address, 
        origin_from_address as Lender, 
        concat ('0x', SUBSTR(topics [3] :: STRING, 27, 40)) as Lender2, 
        TRY_TO_NUMBER(
            public.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA)))::integer
        ) as amount,
        case when Lender = Lender2 then 'no' 
        else 'yes' end as Lender_is_a_contract,
        _inserted_timestamp,
        _log_id
from {{ ref('silver__logs') }}
where topics [0]::string = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a' and tx_hash in (select tx_hash from unlending_txns)
and concat ('0x', SUBSTR(topics [2] :: STRING, 27, 40)) in (select ADDRESS from {{ ref('silver__contracts') }} where name ilike 'kashi Medium Risk%') 
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),

Total as (
select * from Lending
union all
select * from Withdraw
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
        lending
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
a.asset,
a.Lender2 as depositor,
a.lender_is_a_contract,
a.lending_pool_address,
a.event_index,
case when d.decimals is null then a.amount else (a.amount/pow(10,d.decimals)) end as amount,
(a.amount* c.price)/pow(10,d.decimals) as amount_USD,
b.symbol as lending_pool,
d.symbol as symbol,
a._inserted_timestamp,
a._log_id
from Total a
left join token_price c 
on date_trunc('hour',a.block_timestamp) = date_trunc('hour',c.hour)  and a.asset = c.token_address
left join labels b 
on a.Lending_pool_address = b.address
left join labels d
on a.asset = d.address


