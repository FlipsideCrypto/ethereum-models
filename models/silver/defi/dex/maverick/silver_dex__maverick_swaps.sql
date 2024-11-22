{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH pools AS (

    SELECT
        pool_address,
        tokenA,
        tokenB
    FROM
        {{ ref('silver_dex__maverick_pools') }}
),
swaps_base AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.event_index,
        l.contract_address,
        regexp_substr_all(SUBSTR(l.data, 3, len(l.data)), '.{64}') AS l_segmented_data,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [0] :: STRING,
                25,
                40
            )
        ) AS sender_address,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [1] :: STRING,
                25,
                40
            )
        ) AS recipient_address,
        CASE
            WHEN TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    l_segmented_data [2] :: STRING
                )
            ) = 0 THEN FALSE
            ELSE TRUE
        END AS tokenAin,
        CASE
            WHEN TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    l_segmented_data [3] :: STRING
                )
            ) = 0 THEN FALSE
            ELSE TRUE
        END AS exactOutput,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [4] :: STRING
            )
        ) AS amountIn,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [5] :: STRING
            )
        ) AS amountOut,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                l_segmented_data [6] :: STRING
            )
        ) AS activeTick,
        tokenA,
        tokenB,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
        INNER JOIN pools
        ON l.contract_address = pool_address
    WHERE
        l.topics [0] :: STRING = '0x3b841dc9ab51e3104bda4f61b41e4271192d22cd19da5ee6e292dc8e2744f713' --Swap
        AND tx_status = 'SUCCESS'

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
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    event_index,
    contract_address,
    sender_address AS sender,
    recipient_address AS tx_to,
    tokenAin AS token_A_in,
    exactOutput AS exact_output,
    amountIn AS amount_in_unadj,
    amountOut AS amount_out_unadj,
    activeTick AS active_tick,
    CASE
        WHEN token_A_in = TRUE THEN tokenA
        ELSE tokenB
    END AS token_in,
    CASE
        WHEN token_A_in = TRUE THEN tokenB
        ELSE tokenA
    END AS token_out,
    'Swap' AS event_name,
    'maverick' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base
WHERE
    token_in <> token_out
