{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH pools AS (

    SELECT
        pool_address,
        token0,
        token1
    FROM
        {{ ref('silver_dex__kyberswap_v1_dynamic_pools') }}
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
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40)) AS sender_address,
        CONCAT('0x', SUBSTR(l.topics [2] :: STRING, 27, 40)) AS to_address,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [0] :: STRING
            )
        ) AS amount0In,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [1] :: STRING
            )
        ) AS amount1In,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [2] :: STRING
            )
        ) AS amount0Out,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [3] :: STRING
            )
        ) AS amount1Out,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [4] :: STRING
            )
        ) AS feeInPrecision,
        token0,
        token1,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
        INNER JOIN pools p
        ON p.pool_address = l.contract_address
    WHERE
        l.topics [0] :: STRING = '0x606ecd02b3e3b4778f8e97b2e03351de14224efaa5fa64e62200afc9395c2499' --Dynamic Swap

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    event_index,
    sender_address AS sender,
    to_address AS tx_to,
    amount0In,
    amount1In,
    amount0Out,
    amount1Out,
    feeInPrecision AS fee_in_precision,
    token0,
    token1,
    CASE
        WHEN amount0In <> 0
        AND amount1In <> 0
        AND amount0Out <> 0 THEN amount1In
        WHEN amount0In <> 0 THEN amount0In
        WHEN amount1In <> 0 THEN amount1In
    END AS amount_in_unadj,
    CASE
        WHEN amount0Out <> 0 THEN amount0Out
        WHEN amount1Out <> 0 THEN amount1Out
    END AS amount_out_unadj,
    CASE
        WHEN amount0In <> 0
        AND amount1In <> 0
        AND amount0Out <> 0 THEN token1
        WHEN amount0In <> 0 THEN token0
        WHEN amount1In <> 0 THEN token1
    END AS token_in,
    CASE
        WHEN amount0Out <> 0 THEN token0
        WHEN amount1Out <> 0 THEN token1
    END AS token_out,
    'Dynamic Swap' AS event_name,
    'kyberswap-v1' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base
WHERE
    token_in <> token_out
