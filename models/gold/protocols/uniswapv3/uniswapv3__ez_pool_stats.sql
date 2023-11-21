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
    token1_balance,
    COALESCE (
        univ3_pool_stats_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_number', 'pool_address']
        ) }}
    ) AS ez_pool_stats_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM 
    {{ ref('silver__univ3_pool_stats') }}