{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'UNISWAPV3',
    'PURPOSE': 'DEFI, DEX' } } }
) }}

SELECT
    blockchain,
    block_number,
    block_timestamp,
    fee_growth_global0_x128,
    fee_growth_global1_x128,
    pool_address,
    pool_name,
    price_0_1,
    price_1_0,
    protocol_fees_token0_adjusted,
    protocol_fees_token1_adjusted,
    token0_address,
    token1_address,
    token0_symbol,
    token1_symbol,
    tick,
    unlocked,
    virtual_liquidity_adjusted,
    virtual_reserves_token0_adjusted,
    virtual_reserves_token1_adjusted,
    virtual_reserves_token0_usd,
    virtual_reserves_token1_usd,
    token0_balance_adjusted,
    token1_balance_adjusted,
    token0_balance_usd,
    token1_balance_usd,
    token0_balance,
    token1_balance
FROM 
    {{ ref('silver__univ3_pool_stats') }}