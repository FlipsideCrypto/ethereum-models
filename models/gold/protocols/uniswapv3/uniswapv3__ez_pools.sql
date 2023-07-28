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

WITH contracts AS (
    SELECT
        LOWER(address) AS address,
        symbol,
        NAME,
        decimals
    FROM
        {{ ref('silver__contracts') }}
    WHERE
        decimals IS NOT NULL
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
                DISTINCT created_time :: DATE
            FROM
                {{ ref('silver__univ3_pools') }} 
        )
),

FINAL AS (

SELECT
    'ethereum' AS blockchain,
    created_block AS block_number,
    created_time AS block_timestamp,
    created_tx_hash AS tx_hash,
    '0x1f98431c8ad98523631ae4a59f267346ea31f984' AS factory_address,
    token0_address,
    token1_address,
    c0.symbol AS token0_symbol,
    c1.symbol AS token1_symbol,
    c0.name AS token0_name,
    c1.name AS token1_name,
    c0.decimals AS token0_decimals,
    c1.decimals AS token1_decimals,
    p0.price AS token0_price,
    p1.price AS token1_price,
    fee,
    fee_percent,
    COALESCE(
        init_price_1_0_unadj / pow(
            10,
            token1_decimals - token0_decimals
        ),
        0
    ) AS init_price_1_0,
    init_price_1_0 * token1_price AS init_price_1_0_usd,
    init_tick,
    tick_spacing,
    div0(
        token1_price,
        token0_price
    ) AS usd_ratio,
    pool_address,
    CONCAT(
        token0_symbol,
        '-',
        token1_symbol,
        ' ',
        fee,
        ' ',
        tick_spacing
    ) AS pool_name
FROM {{ ref('silver__univ3_pools') }} p 
LEFT JOIN contracts c0
    ON c0.address = p.token0_address
LEFT JOIN contracts c1
    ON c1.address = p.token1_address
LEFT JOIN token_prices p0
    ON p0.token_address = p.token0_address
    AND p0.hour = DATE_TRUNC('hour',created_time)
LEFT JOIN token_prices p1
    ON p1.token_address = p.token1_address
    AND p1.hour = DATE_TRUNC('hour',created_time)
)

SELECT
    blockchain,
    block_number,
    block_timestamp,
    tx_hash,
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
    token1_decimals    
FROM FINAL