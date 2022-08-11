{{ config(
    materialized = 'view'
) }}

SELECT
    block_number, 
    block_timestamp, 
    tx_hash, 
    tx_status, 
    borrower, 
    lender, 
    token_loaned, 
    symbol, 
    amount_loaned_unadjusted, 
    decimals, 
    amount_loaned
FROM 
    {{ ref('silver_maker__flash_loans') }}