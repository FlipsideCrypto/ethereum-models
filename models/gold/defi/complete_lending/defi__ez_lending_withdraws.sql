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
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_hash,
    event_index,
    contract_address,
    event_name,
    protocol_market,
    withdraw_amount as amount,
    withdraw_amount_usd as amount_usd,
    withdraw_asset,
    withdraw_symbol,
    depositor_address AS depositor,
    platform
FROM 
    {{ ref('silver__complete_lending_withdraws') }}