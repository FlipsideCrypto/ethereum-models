{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'COMPOUND, SPARK, AAVE, FRAXLEND',
                'PURPOSE': 'LENDING, DEPOSITS'
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
    deposit_amount as amount,
    deposit_amount_usd as amount_usd,
    deposit_asset,
    symbol as deposit_symbol,
    depositor_address as depositor,
platform
FROM 
    {{ ref('silver__complete_lending_deposits') }}