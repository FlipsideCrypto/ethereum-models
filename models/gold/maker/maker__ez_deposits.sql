{{ config(
    materialized = 'view'
) }}

SELECT 
    block_number, 
    block_timestamp, 
    tx_hash, 
    tx_status, 
    event_index, 
    depositor, 
    vault, 
    token_deposited, 
    symbol, 
    amount_deposited, 
    decimals
FROM 
    {{ ref('silver_maker__deposits') }}