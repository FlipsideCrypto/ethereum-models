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
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    protocol_token,
    borrow_asset,
    borrowed_tokens,
    borrowed_usd,
    borrower_address,
    borrow_rate_mode,
    lending_pool_contract,
    platform,
    symbol,
    blockchain
FROM 
    {{ ref('silver__complete_lending_borrows') }}