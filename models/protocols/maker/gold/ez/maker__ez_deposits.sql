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
    tags = ['non_realtime']
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
    amount_deposited_unadjusted, 
    decimals, 
    amount_deposited
FROM 
    {{ ref('silver_maker__deposits') }}