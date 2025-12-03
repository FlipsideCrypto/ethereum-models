{{ config(
    materialized = "incremental",
    unique_key = "block_number",
    incremental_strategy = "delete+insert",
    cluster_by = "block_timestamp::date",
    tags = ['gold','beacon']
) }}

select 
    substr(data,1,42) as from_address,
    block_number,
    block_timestamp,
    tx_hash,
    '0x' || substr(data,43,96) as pubkey,
    to_number(substr(data, 139, 16), 'XXXXXXXXXXXXXXXX')/1e9 as amount,
    sysdate() as modified_timestamp,
    sysdate() as inserted_timestamp,
    {{dbt_utils.generate_surrogate_key(
        ['block_number', 'tx_hash', 'from_address', 'pubkey', 'amount']
    )}} as fact_manual_withdrawal_requests_id
from {{ ref('core__fact_event_logs') }}
where contract_address = lower('0x00000961Ef480Eb55e80D19ad83579A64c007002')
and block_number >= 22431159
{% if is_incremental() %}
and inserted_timestamp >= (
    select coalesce(max(inserted_timestamp), '1970-01-01') from {{ this }}
)
{% endif %}