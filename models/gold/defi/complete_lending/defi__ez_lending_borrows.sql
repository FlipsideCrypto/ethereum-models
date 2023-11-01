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
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_hash,
    event_index,
    contract_address,
    event_name,
    protocol_market,
    borrow_amount as amount,
    borrow_amount_usd as amount_usd,
    borrow_asset,
    symbol as borrow_symbol,
    borrower_address as borrower,
    platform
FROM 
    {{ ref('silver__complete_lending_borrows') }}