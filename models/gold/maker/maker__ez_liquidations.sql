{{ config(
    materialized = 'view'
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