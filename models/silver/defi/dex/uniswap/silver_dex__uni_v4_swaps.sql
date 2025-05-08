{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH base_swaps AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        RANK() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS event_rank,
        topic_1 AS pool_id,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) AS sender,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [0] :: STRING
            )
        ) AS amount0,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [1] :: STRING
            )
        ) AS amount1,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [2] :: STRING
            )
        ) AS sqrtPriceX96,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [3] :: STRING
            )
        ) AS liquidity,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [4] :: STRING
            )
        ) AS tick,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [4] :: STRING
            )
        ) AS fee,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2025-01-22'
        AND contract_address = '0x000000000004444c5dc75cb358380d2e3de08a90'
        AND topic_0 = '0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f' -- swap
        AND tx_succeeded
        AND event_removed = FALSE

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
pool_data AS (
    SELECT
        token0,
        token1,
        fee,
        pool_id,
        tick_spacing,
        hook_address,
        pool_address,
        beforeSwap,
        beforeSwapReturnDelta,
        afterSwap,
        afterSwapReturnDelta
    FROM
        {{ ref('silver_dex__uni_v4_pools') }}
),
traces_base AS (
    SELECT
        tx_hash,
        trace_index,
        RANK() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) trace_rank,
        LEFT(
            input,
            10
        ) AS method,
        SUBSTR(
            output,
            67,
            64
        ) AS STRING,
        LEFT(
            STRING,
            32
        ) AS high_bits,
        TRY_TO_NUMBER(utils.udf_hex_to_int('s2c', high_bits :: STRING)) AS specified_currency,
        SUBSTR(
            STRING,
            33,
            32
        ) AS low_bits,
        TRY_TO_NUMBER(utils.udf_hex_to_int('s2c', low_bits :: STRING)) AS unspecified_currency,
        regexp_substr_all(SUBSTR(input, 11, len(input)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [5], 25, 40)) AS hook_address,
        TRY_TO_BOOLEAN(utils.udf_hex_to_int(segmented_data [6] :: STRING)) AS zero_for_one,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [7] :: STRING
            )
        ) AS amount_specified,
        CASE
            WHEN amount_specified < 0 THEN TRUE
            ELSE FALSE
        END AS exact_input,
        CASE
            WHEN specified_currency = 0 THEN amount_specified * -1
            ELSE specified_currency
        END AS specified_currency_adj,
        -1 * CASE
            WHEN (
                zero_for_one = TRUE
                AND exact_input = TRUE
            )
            OR (
                zero_for_one = FALSE
                AND exact_input = FALSE
            ) THEN specified_currency
            ELSE unspecified_currency
        END AS amount0,
        -1 * CASE
            WHEN (
                zero_for_one = FALSE
                AND exact_input = TRUE
            )
            OR (
                zero_for_one = TRUE
                AND exact_input = FALSE
            ) THEN specified_currency
            ELSE unspecified_currency
        END AS amount1,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_traces') }}
        t
    WHERE
        block_timestamp :: DATE >= '2025-01-22'
        AND from_address = '0x000000000004444c5dc75cb358380d2e3de08a90'
        AND hook_address IN (
            SELECT
                DISTINCT hook_address
            FROM
                pool_data
        )
        AND LEFT(
            input,
            10
        ) IN (
            '0x575e24b4',
            '0xb47b2fb1'
        )
        AND amount0 <> 0
        AND amount1 <> 0
        AND trace_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
swap_hook AS (
    SELECT
        tx_hash,
        method,
        trace_rank,
        t.hook_address,
        exact_input,
        zero_for_one,
        amount0,
        amount1
    FROM
        traces_base t
        INNER JOIN (
            SELECT
                hook_address,
                beforeSwap,
                beforeSwapReturnDelta,
                afterSwap,
                afterSwapReturnDelta
            FROM
                pool_data
            GROUP BY
                ALL
        ) p
        ON t.hook_address = p.hook_address
    WHERE
        (
            t.method = '0x575e24b4'
            AND p.beforeSwap
            AND p.beforeSwapReturnDelta
        )
        OR (
            t.method = '0xb47b2fb1'
            AND p.afterSwap
            AND p.afterSwapReturnDelta
        )
)
SELECT
    block_number,
    block_timestamp,
    s.tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    s.contract_address,
    pool_address,
    event_index,
    s.pool_id,
    sender,
    sender AS recipient,
    COALESCE(
        sh.amount0,
        s.amount0
    ) AS amount0_unadj,
    COALESCE(
        sh.amount1,
        s.amount1
    ) AS amount1_unadj,
    sqrtPriceX96,
    liquidity,
    tick,
    s.fee,
    token0 AS token0_address,
    token1 AS token1_address,
    tick_spacing,
    CONCAT(
        s.tx_hash :: STRING,
        '-',
        event_index :: STRING
    ) AS _log_id,
    _inserted_timestamp
FROM
    base_swaps s
    INNER JOIN pool_data p
    ON s.pool_id = p.pool_id
    LEFT JOIN swap_hook sh
    ON s.tx_hash = sh.tx_hash
    AND p.hook_address = sh.hook_address
    AND s.event_rank = sh.trace_rank
