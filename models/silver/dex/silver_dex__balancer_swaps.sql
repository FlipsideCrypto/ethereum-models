{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH pool_name AS (

    SELECT
        pool_name,
        poolId,
        pool_address
    FROM
        {{ ref('silver_dex__balancer_pools') }}
),
swaps_base AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        _inserted_timestamp,
        event_name,
        event_index,
        event_inputs :amountIn :: INTEGER AS amountIn,
        event_inputs :amountOut :: INTEGER AS amountOut,
        event_inputs :poolId :: STRING AS poolId,
        event_inputs :tokenIn :: STRING AS token_in,
        event_inputs :tokenOut :: STRING AS token_out,
        SUBSTR(
            event_inputs :poolId :: STRING,
            0,
            42
        ) AS pool_address,
        _log_id,
        ingested_at,
        'balancer' AS platform,
        origin_from_address AS sender,
        origin_from_address AS tx_to
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address = LOWER('0xBA12222222228d8Ba445958a75a0704d566BF2C8')
        AND event_name = 'Swap'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
contracts AS (
    SELECT
        *
    FROM
        {{ ref('core__dim_contracts') }}
    WHERE
        decimals IS NOT NULL
        AND (
            address IN (
                SELECT
                    DISTINCT token_in
                FROM
                    swaps_base
            )
            OR address IN (
                SELECT
                    DISTINCT token_out
                FROM
                    swaps_base
            )
        )
),
hourly_token_price AS (
    SELECT
        HOUR,
        token_address,
        AVG(price) AS price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
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
                swaps_base
        )
    GROUP BY
        1,
        2
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    _inserted_timestamp,
    s.event_name,
    event_index,
    amountIn AS amountIn_unadj,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    CASE
        WHEN decimals_in IS NULL THEN amountIn_unadj
        ELSE (amountIn_unadj / pow(10, decimals_in))
    END AS amount_in,
    CASE
        WHEN decimals_in IS NOT NULL THEN ROUND(
            amount_in * p1.price,
            2
        )
    END AS amount_in_usd,
    amountOut AS amountOut_unadj,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    CASE
        WHEN decimals_out IS NULL THEN amountOut_unadj
        ELSE (amountOut_unadj / pow(10, decimals_out))
    END AS amount_out,
    CASE
        WHEN decimals_out IS NOT NULL THEN ROUND(
            amount_out * p2.price,
            2
        )
    END AS amount_out_usd,
    pn.poolId,
    token_in,
    token_out,
    s.pool_address,
    s._log_id,
    s.ingested_at,
    s.platform,
    sender,
    tx_to,
    pool_name
FROM
    swaps_base s
    LEFT JOIN contracts c1
    ON token_in = c1.address
    LEFT JOIN contracts c2
    ON token_out = c2.address
    LEFT JOIN hourly_token_price p1
    ON token_in = p1.token_address
    AND DATE_TRUNC(
        'hour',
        block_timestamp
    ) = p1.hour
    LEFT JOIN hourly_token_price p2
    ON token_out = p2.token_address
    AND DATE_TRUNC(
        'hour',
        block_timestamp
    ) = p2.hour
    LEFT JOIN pool_name pn
    ON pn.pool_address = s.pool_address
WHERE
    pool_name IS NOT NULL
