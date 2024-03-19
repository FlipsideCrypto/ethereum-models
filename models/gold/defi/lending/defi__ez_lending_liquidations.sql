 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'AAVE, COMPOUND, CREAM, FLUX, FRAXLEND, RADIANT, SILO, SPARK, STRIKE, STURDY, UWU',
                'PURPOSE': 'LENDING, LIQUIDATIONS'
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
    liquidator,
    borrower,
    protocol_market,
    collateral_token,
    collateral_token_symbol,
    amount_unadj,
    amount,
    amount_usd,
    debt_token,
    debt_token_symbol,
    complete_lending_liquidations_id AS ez_lending_liquidations_id,
    inserted_timestamp,
    modified_timestamp
FROM 
    {{ ref('silver__complete_lending_liquidations') }}