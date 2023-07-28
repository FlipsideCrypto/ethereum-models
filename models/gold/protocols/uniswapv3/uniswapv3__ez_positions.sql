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

WITH uni_pools AS (
    SELECT
        token0_address,
        token1_address,
        fee,
        fee_percent,
        tick_spacing,
        pool_address,
        token0_symbol,
        token1_symbol,
        token0_decimals,
        token1_decimals,
        pool_name
    FROM
        {{ ref('uniswapv3__ez_pools') }}
),

token_prices AS (
    SELECT
        HOUR,
        LOWER(token_address) AS token_address,
        price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                {{ ref('silver__univ3_lp_actions') }} 
        )
),

FINAL AS (

SELECT
    blockchain,
    block_number,
    block_timestamp,
    tx_hash,
    a.fee_percent,
    fee_growth_inside0_last_x128,
    fee_growth_inside1_last_x128,
    is_active,
    COALESCE(
            liquidity / pow(10, (token1_decimals + token0_decimals) / 2),
            0
        ) AS liquidity_adjusted,
    liquidity_provider,
    nf_position_manager_address,
    nf_token_id,
    a.pool_address,
    pool_name,
    a.tick_upper,
    a.tick_lower,
    pow(1.0001, (tick_lower)) / pow(10,(token1_decimals - token0_decimals)) AS price_lower_1_0,
    pow(1.0001, (tick_upper)) / pow(10,(token1_decimals - token0_decimals)) AS price_upper_1_0, 
    pow(1.0001, -1 * (tick_upper)) / pow(10,(token0_decimals - token1_decimals)) AS price_lower_0_1, 
    pow(1.0001, -1 * (tick_lower)) / pow(10,(token0_decimals - token1_decimals)) AS price_upper_0_1, 
    price_lower_1_0 * p1.price AS price_lower_1_0_usd, 
    price_upper_1_0 * p1.price AS price_upper_1_0_usd, 
    price_lower_0_1 * p0.price AS price_lower_0_1_usd, 
    price_upper_0_1 * p0.price AS price_upper_0_1_usd,
    COALESCE(
            tokensOwed0 / pow(
                10,
                token0_decimals
            ),
            0
        ) AS tokens_owed0_adjusted,
    COALESCE(
            tokensOwed1 / pow(
                10,
                token1_decimals
            ),
            0
        ) AS tokens_owed1_adjusted,
    COALESCE(
            tokens_owed0_adjusted * p0.price,
            0
        ) AS tokens_owed0_usd,
    COALESCE(
            tokens_owed1_adjusted * p1.price,
            0
        ) AS tokens_owed1_usd,
    a.token0_address,
    a.token1_address,
    token0_symbol,
    token1_symbol
FROM
    {{ ref('silver__univ3_positions') }} a
LEFT JOIN uni_pools p
    ON a.pool_address = p.pool_address
LEFT JOIN token_prices p0
    ON p0.token_address = a.token0_address
        AND p0.hour = DATE_TRUNC('hour',block_timestamp)
LEFT JOIN token_prices p1
    ON p1.token_address = a.token1_address
        AND p1.hour = DATE_TRUNC('hour',block_timestamp)

)

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
FROM FINAL