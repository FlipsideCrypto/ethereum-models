{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    blockchain,
    block_number,
    block_timestamp,
    tx_hash,
    fee_percent,
    fee_growth_inside0_last_x128,
    fee_growth_inside1_last_x128,
    is_active,
    liquidity_adjusted,
    liquidity_provider,
    nf_position_manager_address,
    nf_token_id,
    pool_address,
    pool_name,
    tick_upper,
    tick_lower,
    price_upper_1_0,
    price_lower_1_0,
    price_upper_0_1,
    price_lower_0_1,
    price_upper_1_0_usd,
    price_lower_1_0_usd,
    price_upper_0_1_usd,
    price_lower_0_1_usd,
    tokens_owed0_adjusted,
    tokens_owed1_adjusted,
    tokens_owed0_usd,
    tokens_owed1_usd,
    token0_address,
    token1_address,
    token0_symbol,
    token1_symbol
FROM
    {{ ref('silver__univ3_positions') }}
