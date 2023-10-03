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
        {{ ref('price__ez_hourly_token_prices') }}
    WHERE
        HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                {{ ref('silver__univ3_lp_actions') }} 
        )
                AND 
            (
                token_address in (SELECT DISTINCT(token0_address) from uni_pools)
            OR
                token_address in (SELECT DISTINCT(token1_address) from uni_pools)
            )
),

FINAL AS (
SELECT
    'ethereum' AS blockchain,
    block_number,
    block_timestamp,
    tx_hash,
    action,
    amount0 / pow(
        10,
        token0_decimals
    ) AS amount0_adjusted,
    amount1 / pow(
        10,
        token1_decimals
    ) AS amount1_adjusted,
    amount0_adjusted * p0.price AS amount0_usd,
    amount1_adjusted * p1.price AS amount1_usd, 
    a.token0_address,
    a.token1_address,
    token0_symbol,
    token1_symbol,
    p0.price AS token0_price,
    p1.price AS token1_price,
    liquidity,
    liquidity / pow(10, (token1_decimals + token0_decimals) / 2) AS liquidity_adjusted,
    liquidity_provider,
    nf_position_manager_address,
    nf_token_id,
    a.pool_address,
    pool_name,
    tick_lower,
    tick_upper,
    pow(1.0001, (tick_lower)) / pow(10,(token1_decimals - token0_decimals)) AS price_lower_1_0,
    pow(1.0001, (tick_upper)) / pow(10,(token1_decimals - token0_decimals)) AS price_upper_1_0, 
    pow(1.0001, -1 * (tick_upper)) / pow(10,(token0_decimals - token1_decimals)) AS price_lower_0_1, 
    pow(1.0001, -1 * (tick_lower)) / pow(10,(token0_decimals - token1_decimals)) AS price_upper_0_1, 
    price_lower_1_0 * p1.price AS price_lower_1_0_usd, 
    price_upper_1_0 * p1.price AS price_upper_1_0_usd, 
    price_lower_0_1 * p0.price AS price_lower_0_1_usd, 
    price_upper_0_1 * p0.price AS price_upper_0_1_usd
FROM
    {{ ref('silver__univ3_lp_actions') }} a
INNER JOIN uni_pools p
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
FROM FINAL
