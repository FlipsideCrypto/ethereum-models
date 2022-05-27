{{ config(
    materialized = 'incremental',
    persist_docs ={ "relation": true,
    "columns": true },
    unique_key = '_log_id',
    cluster_by = ['ingested_at::DATE']
) }}

WITH v2_pairs AS (

    SELECT
        pool_address,
        pool_name,
        token0_address,
        token0_decimals,
        token0_symbol,
        token1_address,
        token1_decimals,
        token1_symbol,
        platform
    FROM
        {{ ref('silver_dex__pools') }}
    WHERE
        platform IN (
            'uniswap-v2',
            'sushiswap'
        )
),
swap_events AS (
    SELECT
        block_number,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        block_timestamp,
        tx_hash,
        contract_address,
        event_name,
        TRY_TO_NUMBER(
            event_inputs :amount0In :: STRING
        ) AS amount0In,
        TRY_TO_NUMBER(
            event_inputs :amount1In :: STRING
        ) AS amount1In,
        TRY_TO_NUMBER(
            event_inputs :amount0Out :: STRING
        ) AS amount0Out,
        TRY_TO_NUMBER(
            event_inputs :amount1Out :: STRING
        ) AS amount1Out,
        event_inputs :sender :: STRING AS sender,
        event_inputs :to :: STRING AS tx_to,
        event_index,
        _log_id,
        ingested_at
    FROM
        {{ ref('silver__logs') }}
    WHERE
        event_name = 'Swap'
        AND tx_status = 'SUCCESS'
        AND contract_address IN (
            SELECT
                DISTINCT pool_address
            FROM
                v2_pairs
        )

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(ingested_at) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
hourly_prices AS (
    SELECT
        token_address,
        HOUR,
        AVG(price) AS price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        1 = 1

{% if is_incremental() %}
AND HOUR :: DATE IN (
    SELECT
        DISTINCT block_timestamp :: DATE
    FROM
        swap_events
)
{% else %}
    AND HOUR :: DATE >= '2020-05-05'
{% endif %}
GROUP BY
    token_address,
    HOUR
),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        contract_address,
        event_name,
        CASE
            WHEN amount0In <> 0
            AND amount1In <> 0
            AND token1_decimals IS NOT NULL THEN amount1In / power(
                10,
                token1_decimals
            ) :: FLOAT
            WHEN amount0In <> 0
            AND token0_decimals IS NOT NULL THEN amount0In / power(
                10,
                token0_decimals
            ) :: FLOAT
            WHEN amount1In <> 0
            AND token1_decimals IS NOT NULL THEN amount1In / power(
                10,
                token1_decimals
            ) :: FLOAT
            WHEN amount0In <> 0
            AND token0_decimals IS NULL THEN amount0In
            WHEN amount1In <> 0
            AND token1_decimals IS NULL THEN amount1In
        END AS amount_in,
        CASE
            WHEN amount0Out <> 0
            AND token0_decimals IS NOT NULL THEN amount0Out / power(
                10,
                token0_decimals
            ) :: FLOAT
            WHEN amount1Out <> 0
            AND token1_decimals IS NOT NULL THEN amount1Out / power(
                10,
                token1_decimals
            ) :: FLOAT
            WHEN amount0Out <> 0
            AND token0_decimals IS NULL THEN amount0Out
            WHEN amount1Out <> 0
            AND token1_decimals IS NULL THEN amount1Out
        END AS amount_out,
        sender,
        tx_to,
        event_index,
        _log_id,
        platform,
        ingested_at,
        CASE
            WHEN amount0In <> 0
            AND amount1In <> 0 THEN token1_address
            WHEN amount0In <> 0 THEN token0_address
            WHEN amount1In <> 0 THEN token1_address
        END AS token_in,
        CASE
            WHEN amount0Out <> 0 THEN token0_address
            WHEN amount1Out <> 0 THEN token1_address
        END AS token_out,
        CASE
            WHEN amount0In <> 0
            AND amount1In <> 0 THEN token1_symbol
            WHEN amount0In <> 0 THEN token0_symbol
            WHEN amount1In <> 0 THEN token1_symbol
        END AS symbol_in,
        CASE
            WHEN amount0Out <> 0 THEN token0_symbol
            WHEN amount1Out <> 0 THEN token1_symbol
        END AS symbol_out,
        CASE
            WHEN amount0In <> 0
            AND amount1In <> 0 THEN token1_decimals
            WHEN amount0In <> 0 THEN token0_decimals
            WHEN amount1In <> 0 THEN token1_decimals
        END AS decimals_in,
        CASE
            WHEN amount0Out <> 0 THEN token0_decimals
            WHEN amount1Out <> 0 THEN token1_decimals
        END AS decimals_out,
        token0_decimals,
        token1_decimals,
        token0_symbol,
        token1_symbol,
        pool_name,
        pool_address
    FROM
        swap_events
        LEFT JOIN v2_pairs
        ON swap_events.contract_address = v2_pairs.pool_address
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in,
    CASE
        WHEN decimals_in IS NOT NULL THEN amount_in * pricesIn.price
        ELSE NULL
    END AS amount_in_usd,
    amount_out,
    CASE
        WHEN decimals_out IS NOT NULL THEN amount_out * pricesOut.price
        ELSE NULL
    END AS amount_out_usd,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    _log_id,
    ingested_at
FROM
    FINAL
    LEFT JOIN hourly_prices AS pricesIn
    ON DATE_TRUNC(
        'HOUR',
        block_timestamp
    ) = pricesIn.hour
    AND FINAL.token_in = pricesIn.token_address
    LEFT JOIN hourly_prices AS pricesOut
    ON DATE_TRUNC(
        'HOUR',
        block_timestamp
    ) = pricesOut.hour
    AND FINAL.token_out = pricesOut.token_address
WHERE
    token_in IS NOT NULL
    AND token_out IS NOT NULL
