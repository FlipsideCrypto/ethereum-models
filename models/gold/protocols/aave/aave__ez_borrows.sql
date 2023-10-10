{{ config(
    materialized = 'view',
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'AAVE',
                'PURPOSE': 'DEFI'
            }
        }
    },
    tags = ['non_realtime','reorg'],
    persist_docs ={ "relation": true,
    "columns": true }
) }}
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    aave_market,
    aave_token,
    borrowed_tokens,
    borrowed_usd,
    borrower_address,
    borrow_rate_mode,
    lending_pool_contract,
    aave_version,
    token_price,
    symbol,
    blockchain,
    _log_id,
    _inserted_timestamp
FROM
    {{ref('silver__aave_ez_borrows')}}