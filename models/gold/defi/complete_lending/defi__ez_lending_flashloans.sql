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
    contract_address,
    event_name,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    platform,
    initiator_address AS initiator,
    target_address as target,
    market AS flashloan_token,
    symbol as flashloan_token_symbol,
    protocol_market,
    flashloan_amount,
    flashloan_amount_usd,
    premium_amount,
    premium_amount_usd
FROM 
    {{ ref('silver__complete_lending_flashloans') }}