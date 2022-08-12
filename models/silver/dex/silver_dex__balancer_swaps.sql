{{ config(
    materialized = 'incremental',
    unique_key = 'pool_address'
) }}

WITH vault_creation AS (

    SELECT
        poolId,
        token0,
        token1,
        token2,
        token3,
        token4,
        token5,
        token6,
        token7,
        token_array,
        token0_symbol,
        token0_decimals,
        token1_symbol,
        token1_decimals,
        token2_symbol,
        token2_decimals,
        token3_symbol,
        token3_decimals,
        token4_symbol,
        token4_decimals,
        token5_symbol,
        token5_decimals,
        token6_symbol,
        token6_decimals,
        token7_symbol,
        token7_decimals,
        pool_name,
    FROM
        {{ ref('silver_dex__balancer_pools') }}
),
swaps AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        event_inputs :amountIn :: INTEGER AS amountIn,
        event_inputs :amountOut :: INTEGER AS amountOut,
        event_inputs :poolId :: STRING AS poolId,
        event_inputs :tokenIn :: STRING AS tokenIn,
        event_inputs :tokenOut :: STRING AS tokenOut,
        SUBSTR(
            event_inputs :poolId :: STRING,
            0,
            42
        ) AS pool_address
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_timestamp > '2022-06-29'
        AND contract_address = LOWER('0xBA12222222228d8Ba445958a75a0704d566BF2C8')
        AND event_name = 'Swap'
),
token_prices AS (
    SELECT
        HOUR,
        token_address,
        AVG(price) AS price
    FROM
        core.fact_hourly_token_prices
    WHERE
        token_address IN (
            SELECT
                DISTINCT address
            FROM
                contracts
        )
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                swaps
        )
    GROUP BY
        1,
        2
)
SELECT
    A.tx_hash,
    A.block_timestamp,
    A.block_number,
    A.event_index,
    CASE
        WHEN b.decimals IS NOT NULL THEN A.amountin / pow(
            10,
            b.decimals
        )
        ELSE A.amountin
    END AS amount_in,
    CASE
        WHEN C.decimals IS NOT NULL THEN A.amountout / pow(
            10,
            C.decimals
        )
        ELSE A.amountout
    END AS amount_out,
    CASE
        WHEN b.decimals IS NOT NULL THEN ROUND(
            amount_in * pIn.price,
            2
        )
        ELSE NULL
    END AS amount_in_usd,
    CASE
        WHEN C.decimals IS NOT NULL THEN ROUND(
            amount_out * pOut.price,
            2
        )
        ELSE NULL
    END AS amount_out_usd,
    CONCAT(
        A.tokenIn,
        ' -> ',
        A.tokenOut
    ) AS trade_direction_address,
    CONCAT(
        b.symbol,
        ' -> ',
        C.symbol
    ) AS trade_direction_symbol,
    A.tokenIn AS tokenIn_address,
    b.symbol AS tokenIn_symbol,
    b.decimals AS tokenIn_decimals,
    A.tokenOut AS tokenOut_address,
    C.symbol AS tokenOut_symbol,
    C.decimals AS tokenOut_decimals,
    d.symbol AS pool_symbol,
    vc.token0 AS token0_address,
    vc.token1 AS token1_address,
    vc.token2 AS token2_address,
    vc.token3 AS token3_address,
    vc.token4 AS token4_address,
    A.poolId,
    A.pool_address,
    d.name AS pool_name
FROM
    swaps A
    LEFT JOIN contracts b
    ON A.tokenIn = b.address
    LEFT JOIN contracts C
    ON A.tokenOut = C.address
    LEFT JOIN contracts d
    ON A.pool_address = d.address
    LEFT JOIN vault_creation vc
    ON A.poolid = vc.poolid
    LEFT JOIN token_prices pIn
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = pIn.hour
    AND pIn.token_address = A.tokenIn
    LEFT JOIN token_prices pOut
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = pOut.hour
    AND pOut.token_address = A.tokenOut
ORDER BY
    block_timestamp DESC,
    tx_hash,
    event_index ASC
