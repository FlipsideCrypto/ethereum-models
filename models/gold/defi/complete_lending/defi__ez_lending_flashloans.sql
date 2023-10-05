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
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    protocol_token,
    market,
    flashloan_amount,
    flashloan_amount_usd,
    premium_amount,
    premium_amount_usd,
    initiator_address,
    target_address,
    platform,
    symbol,
    blockchain
FROM 
    {{ ref('silver__complete_lending_flashloans') }}