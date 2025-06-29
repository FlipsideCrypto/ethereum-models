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
    token1_symbol,
    COALESCE (
        univ3_positions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS ez_positions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__univ3_positions') }}