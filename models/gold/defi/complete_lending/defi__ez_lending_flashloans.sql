 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'SPARK, AAVE',
                'PURPOSE': 'LENDING, FLASHLOANS'
            }
        }
    }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    event_name,
    protocol_market,
    flashloan_amount,
    flashloan_amount_usd,
    market AS flashloan_asset,
    symbol as flashloan_symbol,
    premium_amount,
    premium_amount_usd,
    initiator_address,
    target_address,
    platform
FROM 
    {{ ref('silver__complete_lending_flashloans') }}