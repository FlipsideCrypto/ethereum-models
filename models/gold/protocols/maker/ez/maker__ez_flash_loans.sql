{{ config(
    materialized = 'view',
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'MAKER, MKR',
                'PURPOSE': 'DEFI'
            }
        }
    },
    persist_docs ={ "relation": true,
    "columns": true }
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