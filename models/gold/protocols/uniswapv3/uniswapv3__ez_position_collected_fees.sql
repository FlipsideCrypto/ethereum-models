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
    event_index,
    pool_address,
    pool_name,
    liquidity_provider,
    nf_token_id,
    nf_position_manager_address,
    token0_symbol,
    token1_symbol,
    amount0_adjusted,
    amount1_adjusted,
    amount0_usd,
    amount1_usd,
    tick_lower,
    tick_upper,
    price_lower,
    price_upper,
    price_lower_usd,
    price_upper_usd
FROM 
    {{ ref('silver__univ3_position_collected_fees') }}