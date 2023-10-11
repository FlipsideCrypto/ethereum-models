 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'COMPOUND, SPARK, AAVE, FRAXLEND',
                'PURPOSE': 'LENDING, WITHDRAWS'
            }
        }
    }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    protocol_token,
    withdraw_asset,
    withdraw_symbol,
    withdraw_amount,
    withdraw_amount_usd,
    depositor_address AS depositor,
    platform,
    blockchain
FROM 
    {{ ref('silver__complete_lending_withdraws') }}