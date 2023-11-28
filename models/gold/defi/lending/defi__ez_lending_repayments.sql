 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'COMPOUND, SPARK, AAVE, FRAXLEND',
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
    payer_address AS payer,
    borrower_address AS borrower,
    protocol_market,
    repay_asset AS token_address,
    repay_symbol AS token_symbol,
    repay_amount_unadj AS amount_unadj,
    repay_amount AS amount,
    repay_amount_usd AS amount_usd
FROM 
    {{ ref('silver__complete_lending_repayments') }}