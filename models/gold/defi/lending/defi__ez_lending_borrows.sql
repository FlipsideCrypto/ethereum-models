 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'AAVE, COMPOUND, CREAM, FLUX, FRAXLEND, MORPHO, RADIANT, SILO, SPARK, STRIKE, STURDY, UWU',
                'PURPOSE': 'LENDING, BORROWS'
            }
        }
    },
    tags = ['gold','defi','lending','curated','ez']
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
    protocol_market,
    borrower,
    token_address,
    token_symbol,
    amount_unadj,
    amount,
    amount_usd,
    complete_lending_borrows_id AS ez_lending_borrows_id,
    inserted_timestamp,
    modified_timestamp
FROM 
    {{ ref('silver__complete_lending_borrows') }}