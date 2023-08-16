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
    event_index, 
    payer, 
    vault, 
    token_paid, 
    symbol, 
    amount_paid_unadjusted,
    decimals, 
    amount_paid
FROM 
    {{ ref('silver_maker__repayments') }}