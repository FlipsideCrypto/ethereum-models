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
    contract_address,
    event_name,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    platform,
    protocol_market,
    borrower_address as borrower,
    borrow_asset as token_address,
    symbol as token_symbol,
    borrow_amount as amount,
    borrow_amount_usd as amount_usd
FROM 
    {{ ref('silver__complete_lending_borrows') }}