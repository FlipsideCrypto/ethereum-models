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
    created_block AS block_number,
    created_time AS block_timestamp,
    created_tx_hash AS tx_hash,
    factory_address,
    fee,
    fee_percent,
    init_price_1_0,
    init_price_1_0_usd,
    init_tick,
    pool_address,
    pool_name,
    tick_spacing,
    token0_address,
    token1_address,
    token0_symbol,
    token1_symbol,
    token0_name,
    token1_name,
    token0_decimals,
    token1_decimals,
    COALESCE (
        univ3_pools_id,
        {{ dbt_utils.generate_surrogate_key(
            ['pool_address']
        ) }}
    ) AS ez_pools_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp   
FROM 
    {{ ref('silver__univ3_pools') }}