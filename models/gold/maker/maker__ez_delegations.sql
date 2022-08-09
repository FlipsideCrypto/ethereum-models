{{ config(
    materialized = 'view'
) }}

SELECT 
    block_number, 
    block_timestamp, 
    tx_hash, 
    tx_status, 
    origin_from_address, 
    contract_address, 
    tx_event, 
    delegate, 
    amount_delegated, 
    decimals
FROM 
    {{ ref('silver_maker__delegations') }}