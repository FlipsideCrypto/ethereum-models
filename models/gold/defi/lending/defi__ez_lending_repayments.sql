 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'AAVE, COMPOUND, CREAM, FLUX, FRAXLEND, RADIANT, SILO, SPARK, STRIKE, STURDY, UWU',
                'PURPOSE': 'LENDING, REPAYMENTS'
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
    platform,
    payer,
    borrower,
    protocol_market,
    token_address,
    token_symbol,
    amount_unadj,
    amount,
    amount_usd,
    complete_lending_repayments_id AS ez_lending_repayments_id,
    inserted_timestamp,
    modified_timestamp
FROM 
    {{ ref('silver__complete_lending_repayments') }}