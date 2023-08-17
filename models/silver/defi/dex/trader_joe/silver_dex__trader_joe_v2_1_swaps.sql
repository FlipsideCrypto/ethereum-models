{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime']
) }}

WITH pools AS (

    SELECT
        lb_pair,
        tokenX,
        tokenY
    FROM
        {{ ref('silver_dex__trader_joe_v2_pools') }}
),
swaps_base AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.origin_function_signature,
        l.origin_from_address,
        l.origin_to_address,
        l.tx_hash,
        l.event_index,
        l.contract_address,
        l.topics,
        l.data,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS l_segmented_data,
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40)) AS sender_address,
        CONCAT('0x', SUBSTR(l.topics [2] :: STRING, 27, 40)) AS to_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [0] :: STRING
            )
        ) AS id,
        l_segmented_data [1] :: STRING AS amountsIn,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(SUBSTR(amountsIn, 0, 32))
        ) AS amount0In,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(SUBSTR(amountsIn, 33, 32))
        ) AS amount1In,
        l_segmented_data [2] :: STRING AS amountsOut,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(SUBSTR(amountsOut, 0, 32))
        ) AS amount0Out,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(SUBSTR(amountsOut, 33, 32))
        ) AS amount1Out,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [3] :: STRING
            )
        ) AS volatilityAccumulated,
        l_segmented_data [4] :: STRING AS totalFees,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(SUBSTR(totalFees, 0, 32))
        ) AS fee0,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(SUBSTR(totalFees, 33, 32))
        ) AS fee1,
        l_segmented_data [5] :: STRING AS protocolFees,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(SUBSTR(protocolFees, 0, 32))
        ) AS protocolFee0,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(SUBSTR(protocolFees, 33, 32))
        ) AS protocolFee1,
        tokenX,
        tokenY,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
        INNER JOIN pools p
        ON lb_pair = l.contract_address
    WHERE
        topics [0] :: STRING = '0xad7d6f97abf51ce18e17a38f4d70e975be9c0708474987bb3e26ad21bd93ca70' --Swap

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
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    event_index,
    contract_address,
    sender_address AS sender,
    to_address AS tx_to,
    id,
    amount0In,
    amount1In,
    amount0Out,
    amount1Out,
    volatilityAccumulated AS volatility_accumulated,
    fee0,
    fee1,
    protocolFee0 AS protocol_fee0,
    protocolFee1 AS protocol_fee1,
    tokenX,
    tokenY,
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
        AND amount0Out <> 0 THEN tokenX
        WHEN amount0In <> 0 THEN tokenY
        WHEN amount1In <> 0 THEN tokenX
    END AS token_in,
    CASE
        WHEN amount0Out <> 0 THEN tokenY
        WHEN amount1Out <> 0 THEN tokenX
    END AS token_out,
    'Swap' AS event_name,
    'trader-joe-v2' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base
WHERE
    token_in <> token_out
