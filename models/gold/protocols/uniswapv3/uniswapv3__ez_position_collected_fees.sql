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
    },
    tags = ['gold','uniswap','curated']
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
    price_upper_usd,
    COALESCE (
        univ3_position_collected_fees_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS ez_position_collected_fees_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM 
    {{ ref('silver__univ3_position_collected_fees') }}