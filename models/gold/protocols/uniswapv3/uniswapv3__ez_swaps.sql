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

{# WITH uni_pools AS (
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
                {{ ref('silver__univ3_swaps') }} 
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
        s.blockchain,
        s.block_number,
        s.block_timestamp,
        s.tx_hash,
        s.pool_address,
        p.pool_name,
        recipient,
        sender,
        tick,
        liquidity,
        COALESCE(
            liquidity / pow(10,((token0_decimals + token1_decimals) / 2)),0
                ) AS liquidity_adjusted,
        event_index,
        amount0_unadj / pow(
            10,
            COALESCE(
                token0_decimals,
                18
            )
        ) AS amount0_adjusted,
        amount1_unadj / pow(
            10,
            COALESCE(
                token1_decimals,
                18
            )
        ) AS amount1_adjusted,
        COALESCE(div0(ABS(amount1_adjusted), ABS(amount0_adjusted)), 0) AS price_1_0,
        COALESCE(div0(ABS(amount0_adjusted), ABS(amount1_adjusted)), 0) AS price_0_1,
        p0.price AS token0_price,
        p1.price AS token1_price,
        CASE
            WHEN token0_decimals IS NOT NULL THEN ROUND(
                token0_price * amount0_adjusted,
                2
            )
        END AS amount0_usd,
        CASE
            WHEN token1_decimals IS NOT NULL THEN ROUND(
                token1_price * amount1_adjusted,
                2
            )
        END AS amount1_usd,
        s.token0_address,
        s.token1_address,
        token0_symbol,
        token1_symbol
    FROM
        {{ ref('silver__univ3_swaps') }} s 
    LEFT JOIN uni_pools p
        ON s.pool_address = p.pool_address
    LEFT JOIN token_prices p0
        ON p0.token_address = s.token0_address
            AND p0.hour = DATE_TRUNC('hour',block_timestamp)
    LEFT JOIN token_prices p1
        ON p1.token_address = s.token1_address
            AND p1.hour = DATE_TRUNC('hour',block_timestamp)
) #}

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
    amount1_usd
FROM
    {{ ref('silver__univ3_swaps') }} s
