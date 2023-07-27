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
    tags = ['non_realtime']
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
                {{ ref('silver__univ3_position_collected_fees') }} 
        )
),

FINAL AS (
SELECT
    blockchain,
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    a.pool_address,
    pool_name,
    liquidity_provider,
    nf_token_id,
    nf_position_manager_address,
    token0_symbol,
    token1_symbol,
    amount0 / pow(10,token0_decimals) AS amount0_adjusted,
    amount1 / pow(10,token1_decimals) AS amount1_adjusted,
    ROUND(amount0_adjusted * p0.price,2) AS amount0_usd,
    ROUND(amount1_adjusted * p1.price,2) AS amount1_usd,
    tick_lower,
    tick_upper,
    pow(1.0001,tick_lower) / pow(10,(token1_decimals - token0_decimals)) AS price_lower,
    pow(1.0001,tick_upper) / pow(10,(token1_decimals - token0_decimals)) AS price_upper,
    ROUND(price_lower * p1.price,2) AS price_lower_usd,
    ROUND(price_upper * p1.price,2) AS price_upper_usd
FROM
    {{ ref('silver__univ3_position_collected_fees') }} a
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
FROM FINAL