{# {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
    'database_tags':{
        'table': {
            'PROTOCOL': '',
            'PURPOSE': 'BRIDGE',
            }
        }
    }
) }}


    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    sender,
    receiver,
    source_chain,
    destination_chain,
    token_address,
    token_symbol,
    amount,
    fee #}