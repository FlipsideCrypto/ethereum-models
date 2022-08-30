{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    'ethereum' AS blockchain,
    created_block AS block_number,
    created_time AS block_timestamp,
    created_tx_hash AS tx_hash,
    '0x1f98431c8ad98523631ae4a59f267346ea31f984' AS factory_address,
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
    token1_decimals
FROM
    {{ ref('silver__univ3_pools') }}
