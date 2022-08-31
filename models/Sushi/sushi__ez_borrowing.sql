{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id',
  cluster_by = ['block_timestamp::DATE']
) }}

with borrow_txns as (
select distinct tx_hash,contract_address
from {{ ref('silver__logs') }}
where topics [0]::string = '0x3a5151e57d3bc9798e7853034ac52293d1a0e12a2b44725e75b03b21f86477a6'
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),

Repay_txns as (
select distinct tx_hash,contract_address
from {{ ref('silver__logs') }}
where topics [0]::string = '0xc8e512d8f188ca059984b5853d2bf653da902696b8512785b182b2c813789a6e'
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),

Add_asset as (
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

Remove_asset as (
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


Borrow as (
select  block_timestamp,
        block_number,
        tx_hash, 
        'Borrow' as action, 
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        event_index,
        concat ('0x', SUBSTR(topics [1] :: STRING, 27, 40)) as asset,
        concat ('0x', SUBSTR(topics [2] :: STRING, 27, 40)) as Lending_pool_address, 
        origin_from_address as Borrower, 
        concat ('0x', SUBSTR(topics [3] :: STRING, 27, 40)) as Borrower2,  
        TRY_TO_NUMBER(
            public.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA)))::integer
        ) as amount,
        case when Borrower = Borrower2 then 'no' 
        else 'yes' end as Borrower_is_a_contract,
        _inserted_timestamp,
        _log_id
from {{ ref('silver__logs') }}
where topics [0]::string = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'  and tx_hash in (select tx_hash from borrow_txns)
and CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) in (select ADDRESS from {{ ref('silver__contracts') }} where name ilike 'kashi Medium Risk%' )
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),

add_coll_same_txn as (
select  block_timestamp,
        block_number,
        tx_hash, 
        'add collateral' as action, 
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        event_index, 
        concat ('0x', SUBSTR(topics [1] :: STRING, 27, 40))  as asset, 
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) as Lending_pool_address, 
        origin_from_address as Borrower, 
        concat ('0x', SUBSTR(topics [2] :: STRING, 27, 40)) as Borrower2,  
        TRY_TO_NUMBER(
            public.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA)))::integer
        ) as amount,
        case when Borrower = Borrower2 then 'no' 
        else 'yes' end as Borrower_is_a_contract,
        _inserted_timestamp,
        _log_id
from {{ ref('silver__logs') }}
where topics [0]::string = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'  and tx_hash in (select tx_hash from borrow_txns)
and  CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) in (select ADDRESS from {{ ref('silver__contracts') }} where name ilike 'kashi Medium Risk%')
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) as asset, 
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) as Lending_pool_address, 
        origin_from_address as Borrower, 
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) as Borrower2, 
        TRY_TO_NUMBER(
            public.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA)))::integer
        ) as amount,
        case when Borrower = Borrower2 then 'no' 
        else 'yes' end as Borrower_is_a_contract,
        _inserted_timestamp,
        _log_id
from {{ ref('silver__logs') }}
where topics [0]::string = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a' and tx_hash in (select tx_hash from Repay_txns)
and CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) in (select ADDRESS from {{ ref('silver__contracts') }} where name ilike 'kashi Medium Risk%') 
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),

remove_coll_same_txn as (
select  block_timestamp, 
        block_number,
        tx_hash, 
        'Remove collateral' as action,
        origin_from_address,
        origin_to_address,
        origin_function_signature, 
        event_index,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) as asset, 
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) as Lending_pool_address, 
        origin_from_address as Borrower, 
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) as Borrower2, 
        TRY_TO_NUMBER(
            public.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA)))::integer
        ) as amount,
        case when Borrower = Borrower2 then 'no' 
        else 'yes' end as Borrower_is_a_contract,
        _inserted_timestamp,
        _log_id
from {{ ref('silver__logs') }}
where topics [0]::string = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'  and tx_hash in (select tx_hash from Repay_txns)
and  CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) in (select ADDRESS from {{ ref('silver__contracts') }} where name ilike 'kashi Medium Risk%')
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),


add_coll_in_separate_txn as (
select  block_timestamp, 
        block_number,
        tx_hash, 
        'add collateral' as action,
        origin_from_address,
        origin_to_address,
        origin_function_signature, 
        event_index,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) as asset, 
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) as Lending_pool_address, 
        origin_from_address as Borrower, 
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) as Borrower2, 
        TRY_TO_NUMBER(
            public.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA)))::integer
        ) as amount,
        case when Borrower = Borrower2 then 'no' 
        else 'yes' end as Borrower_is_a_contract,
        _inserted_timestamp,
        _log_id
from ETHEREUM_DEV.silver.logs
where topics [0]::string = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'  
and tx_hash not in (select tx_hash from borrow_txns) 
and tx_hash not in (select tx_hash from Repay_txns)
and tx_hash not in (select tx_hash from Add_asset)
and  CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) in (select ADDRESS from ETHEREUM_DEV.silver.contracts where name ilike 'kashi Medium Risk%')
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),

Remove_coll_in_separate_txn as (
select  block_timestamp, 
        block_number,
        tx_hash, 
        'Remove collateral' as action,
        origin_from_address,
        origin_to_address,
        origin_function_signature, 
        event_index,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) as asset, 
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) as Lending_pool_address, 
        origin_from_address as Borrower, 
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) as Borrower2, 
        TRY_TO_NUMBER(
            public.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA)))::integer
        ) as amount,
        case when Borrower = Borrower2 then 'no' 
        else 'yes' end as Borrower_is_a_contract,
        _inserted_timestamp,
        _log_id
from ETHEREUM_DEV.silver.logs
where topics [0]::string = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'  
and tx_hash not in (select tx_hash from borrow_txns) 
and tx_hash not in (select tx_hash from Repay_txns)
and tx_hash not in (select tx_hash from Remove_asset)
and  CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) in (select ADDRESS from ETHEREUM_DEV.silver.contracts where name ilike 'kashi Medium Risk%')
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
select * from Borrow
union all
select * from add_coll_same_txn
union all
select * from remove_coll_same_txn
union all
select * from Repay
union all
select * from add_coll_in_separate_txn
union all
select * from Remove_coll_in_separate_txn
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
a.Borrower2 as Borrower,
a.Borrower_is_a_contract,
a.lending_pool_address,
b.symbol as lending_pool,
a.asset,
d.symbol as symbol,
case when d.decimals is null then a.amount else (a.amount/pow(10,d.decimals)) end as amount,
(a.amount* c.price)/pow(10,d.decimals) as amount_USD,
a._log_id,
a._inserted_timestamp
from Total a
left join token_price c 
on date_trunc('hour',a.block_timestamp) = date_trunc('hour',c.hour)  and a.asset = c.token_address
left join labels b 
on a.Lending_pool_address = b.address
left join labels d
on a.asset = d.address



