{{ config(
    materialized = 'view',
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'MAKER, MKR',
                'PURPOSE': 'DEFI'
            }
        }
    },
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT 
    block_number, 
    block_timestamp, 
    tx_hash, 
    tx_status, 
    collateral, 
    symbol, 
    collateral_balance_unadjusted, 
    decimals, 
    collateral_balance, 
    normalized_stablecoin_debt, 
    vault, 
    liquidated_wallet, 
    liquidator, 
    auction_id
FROM 
    {{ ref('silver_maker__liquidations') }}