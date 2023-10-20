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
    tx_hash,
    event_index,
    event_name,
    protocol_market,
    repay_token,
    repay_amount,
    repay_amount_usd,
    repay_symbol,
    payer_address AS payer,
    borrower_address AS borrower,
    platform
FROM 
    {{ ref('silver__complete_lending_repayments') }}