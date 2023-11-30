{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'UNISWAPV3',
                'PURPOSE': 'DEFI, DEX'
            }
        }
    }
) }}

SELECT
    blockchain,
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
    price_upper_0_1_usd,
    COALESCE (
        univ3_lp_actions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS ez_lp_actions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__univ3_lp_actions') }}
