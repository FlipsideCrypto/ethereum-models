 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'COMPOUND, SPARK, AAVE, FRAXLEND',
                'PURPOSE': 'LENDING, BORROWS'
            }
        }
    }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    protocol_market,
    borrow_asset,
    borrow_amount,
    borrow_amount_usd,
    symbol as borrow_symbol,
    borrower_address,
    platform
FROM 
    {{ ref('silver__complete_lending_borrows') }}