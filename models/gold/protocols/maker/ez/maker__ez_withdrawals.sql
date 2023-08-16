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
    withdrawer, 
    vault, 
    token_withdrawn, 
    symbol, 
    amount_withdrawn_unadjusted, 
    decimals, 
    amount_withdrawn
FROM 
    {{ ref('silver_maker__withdrawals') }}