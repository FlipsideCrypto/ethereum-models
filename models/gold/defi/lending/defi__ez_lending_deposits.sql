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
    tx_hash,
    contract_address,
    event_name,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    platform,
    protocol_market,
    depositor_address as depositor,
    deposit_asset as token_address,
    symbol as token_symbol,
    deposit_amount_unadj AS amount_unadj,
    deposit_amount as amount,
    deposit_amount_usd as amount_usd,
    COALESCE (
        complete_lending_deposits_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS ez_lending_deposits_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM 
    {{ ref('silver__complete_lending_deposits') }}