{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH pool_data AS (

    SELECT
        token0,
        token1,
        fee,
        pool_id,
        tick_spacing,
        hook_address,
        pool_address,
    FROM
        {{ ref('silver_dex__uni_v4_pools') }}
),
events_swap AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        topic_1 AS pool_id,
        pool_address,
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
        utils.udf_hex_to_int(
            's2c',
            segmented_data [2] :: STRING
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
        modified_timestamp AS _inserted_timestamp,
        CONCAT(
            token0,
            token1,
            tick_spacing,
            hook_address
        ) AS proxy_id,
        RANK() over (
            PARTITION BY tx_hash,
            proxy_id
            ORDER BY
                event_index ASC
        ) AS event_rank,
        token0,
        token1,
        tick_spacing
    FROM
        {{ ref('core__fact_event_logs') }}
        LEFT JOIN pool_data p
        ON topic_1 = p.pool_id
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
traces_swap AS (
    SELECT
        tx_hash,
        trace_index,
        regexp_substr_all(SUBSTR(input, 11, len(input)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS currency0,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS currency1,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [2] :: STRING
            )
        ) AS fee,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [3] :: STRING
            )
        ) AS tick_spacing,
        CONCAT('0x', SUBSTR(segmented_data [4] :: STRING, 25, 40)) AS hook_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [6] :: STRING
            )
        ) AS amount_specified,
        TRY_TO_BOOLEAN(utils.udf_hex_to_int(segmented_data [5] :: STRING)) AS zero_for_one,
        CASE
            WHEN amount_specified < 0 THEN TRUE
            ELSE FALSE
        END AS exact_input,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [7] :: STRING
        ) AS sqrtPriceX96,
        SUBSTR(
            output,
            3,
            32
        ) AS high_bits,
        TRY_TO_NUMBER(utils.udf_hex_to_int('s2c', high_bits :: STRING)) AS amount0,
        SUBSTR(
            output,
            32 + 3,
            32
        ) AS low_bits,
        TRY_TO_NUMBER(utils.udf_hex_to_int('s2c', low_bits :: STRING)) AS amount1,
        modified_timestamp AS _inserted_timestamp,
        CONCAT(
            currency0,
            currency1,
            tick_spacing,
            hook_address
        ) AS proxy_id,
        RANK() over (
            PARTITION BY tx_hash,
            proxy_id
            ORDER BY
                trace_index ASC
        ) AS trace_rank
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        block_timestamp :: DATE >= '2025-01-22'
        AND to_address = LOWER('0x000000000004444c5dc75cB358380D2e3dE08A90')
        AND LEFT(
            input,
            10
        ) = '0xf3cd914c' -- swap
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
)
SELECT
    e.block_number,
    e.block_timestamp,
    e.tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_address,
    pool_id,
    event_index,
    sender,
    sender AS recipient,
    t.amount0 AS amount0_unadj,
    t.amount1 AS amount1_unadj,
    e.sqrtPriceX96,
    liquidity,
    tick,
    e.fee,
    token0 AS token0_address,
    token1 AS token1_address,
    e.tick_spacing,
    CONCAT(
        e.tx_hash :: STRING,
        '-',
        event_index :: STRING
    ) AS _log_id,
    e._inserted_timestamp
FROM
    events_swap e
    INNER JOIN traces_swap t
    ON e.tx_hash = t.tx_hash
    AND trace_rank = event_rank
    AND t.proxy_id = e.proxy_id
