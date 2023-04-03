{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'UNISWAPV3',
    'PURPOSE': 'DEFI, DEX' }} }
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
                {{ ref('silver__univ3_pool_stats') }} 
        )
),

FINAL AS (
SELECT
    'ethereum' AS blockchain,
    block_number,
    block_timestamp,
    feeGrowthGlobal0X128 AS fee_growth_global0_x128,
    feeGrowthGlobal1X128 AS fee_growth_global1_x128,
    a.pool_address,
    pool_name,
    pow(1.0001,tick) / pow(10,token1_decimals - token0_decimals
        ) AS price_1_0,
    1 / price_1_0 AS price_0_1,
    COALESCE(
        token0_protocol_fees / pow(10,token0_decimals),
        0
        ) AS protocol_fees_token0_adjusted, 
    COALESCE(
        token1_protocol_fees / pow(10,token1_decimals),
        0
        ) AS protocol_fees_token1_adjusted,
    a.token0_address,
    a.token1_address,
    token0_symbol,
    token1_symbol,
    tick,
    unlocked,
    COALESCE(
        liquidity / pow(10,(token1_decimals + token0_decimals) / 2),
        0
        ) AS virtual_liquidity_adjusted,
    div0(
        liquidity,
        sqrt_hp
    ) / pow(
        10,
        token0_decimals
        ) AS virtual_reserves_token0_adjusted,
    (
        liquidity * sqrt_hp
    ) / pow(
        10,
        token1_decimals
        ) AS virtual_reserves_token1_adjusted,
    virtual_reserves_token0_adjusted * p0.price AS virtual_reserves_token0_usd,
    virtual_reserves_token1_adjusted * p1.price AS virtual_reserves_token1_usd,
    token0_balance,
    token1_balance,
    token0_balance / pow(10,token0_decimals
        ) AS token0_balance_adjusted,
    token1_balance / pow(10,token1_decimals
        ) AS token1_balance_adjusted,
    token0_balance_adjusted * p0.price AS token0_balance_usd,
    token1_balance_adjusted * p1.price AS token1_balance_usd
FROM
    {{ ref('silver__univ3_pool_stats') }} a
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
    fee_growth_global0_x128,
    fee_growth_global1_x128,
    pool_address,
    pool_name,
    price_0_1,
    price_1_0,
    protocol_fees_token0_adjusted,
    protocol_fees_token1_adjusted,
    token0_address,
    token1_address,
    token0_symbol,
    token1_symbol,
    tick,
    unlocked,
    virtual_liquidity_adjusted,
    virtual_reserves_token0_adjusted,
    virtual_reserves_token1_adjusted,
    virtual_reserves_token0_adjusted * p0.price AS virtual_reserves_token0_usd,
    virtual_reserves_token1_adjusted * p1.price AS virtual_reserves_token1_usd,
    token0_balance_adjusted,
    token1_balance_adjusted,
    token0_balance_adjusted * p0.price AS token0_balance_usd,
    token1_balance_adjusted * p1.price AS token1_balance_usd,
    token0_balance,
    token1_balance
FROM FINAL