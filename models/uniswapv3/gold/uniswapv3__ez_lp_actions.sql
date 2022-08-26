{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    'ethereum' AS blockchain,
    block_number,
    block_timestamp,
    tx_hash,
    action,
    amount0_adjusted,
    amount1_adjusted,
    amount0_usd,
    amount1_usd,
    token0_address,
    token1_address,
    token0_symbol,
    token1_symbol,
    token0_price,
    token1_price,
    liquidity,
    liquidity_adjusted,
    liquidity_provider,
    nf_position_manager_address,
    nf_token_id,
    pool_address,
    pool_name,
    tick_lower,
    tick_upper,
    price_lower_1_0,
    price_upper_1_0,
    price_lower_0_1,
    price_upper_0_1,
    price_lower_1_0_usd,
    price_upper_1_0_usd,
    price_lower_0_1_usd,
    price_upper_0_1_usd
FROM
    {{ ref('silver__univ3_lp_actions') }}
