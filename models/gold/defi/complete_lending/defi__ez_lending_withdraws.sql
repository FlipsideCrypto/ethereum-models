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
    contract_address,
    event_name,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    platform,
    depositor_address AS depositor,
    protocol_market,
    withdraw_asset AS token_address,
    withdraw_symbol AS token_symbol,
    withdraw_amount as amount,
    withdraw_amount_usd as amount_usd
FROM 
    {{ ref('silver__complete_lending_withdraws') }}