{{ config(
    materialized = 'view'
) }}

SELECT 
    block_number, 
    block_timestamp, 
    tx_hash, 
    tx_status, 
    event_index, 
    payer, 
    vault, 
    token_paid, 
    symbol, 
    amount_paid,
    decimals
FROM 
    {{ ref('silver_maker__repayments') }}