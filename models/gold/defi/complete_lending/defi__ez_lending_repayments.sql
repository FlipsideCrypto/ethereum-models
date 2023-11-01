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
    repay_asset as token_address,
    repay_symbol as token_symbol,
    repay_amount as amount,
    repay_amount_usd as amount_usd
FROM 
    {{ ref('silver__complete_lending_repayments') }}