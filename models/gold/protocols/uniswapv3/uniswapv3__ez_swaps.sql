{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'UNISWAPV3',
                'PURPOSE': 'DEFI, DEX, SWAPS'
            }
        }
    }
) }}

SELECT
    blockchain,
    block_number,
    block_timestamp,
    tx_hash,
    pool_address,
    pool_name,
    price_1_0,
    price_0_1,
    recipient,
    sender,
    tick,
    liquidity,
    liquidity_adjusted,
    event_index,
    amount0_adjusted,
    amount1_adjusted,
    token0_address,
    token1_address,
    token0_symbol,
    token1_symbol,
    token0_price,
    token1_price,
    amount0_usd,
    amount1_usd,
    COALESCE (
        univ3_swaps_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS ez_swaps_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__univ3_swaps') }} 