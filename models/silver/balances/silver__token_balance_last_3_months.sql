{{ config(
    materialized = 'table',
    unique_key = 'id',
    cluster_by = ['block_timestamp::date'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['curated']
) }}

select 
    address,
    contract_address,
    balance,
    block_number,
    block_timestamp,
    _inserted_timestamp
from 
    {{ ref('silver__token_balances') }}
where 
    block_timestamp >= DATEADD('month', -12, SYSDATE())
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY address, contract_address 
    ORDER BY block_number DESC
) <= 2