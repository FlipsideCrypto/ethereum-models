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
    withdrawn_tokens,
    withdrawn_usd,
    depositor_address,
    aave_version,
    token_price,
    symbol,
    blockchain,
    _log_id,
    _inserted_timestamp
FROM
    {{ref('silver__aave_withdraws')}}