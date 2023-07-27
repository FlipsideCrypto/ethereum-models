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
        {{ ref('silver_dex__kyberswap_v2_elastic_pools') }}
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
        CONCAT('0x', SUBSTR(l.topics [2] :: STRING, 27, 40)) AS reipient_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                l_segmented_data [0] :: STRING
            )
        ) AS deltaQty0,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                l_segmented_data [1] :: STRING
            )
        ) AS deltaQty1,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [2] :: STRING
            )
        ) AS sqrtP,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [3] :: STRING
            )
        ) AS liquidity,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                l_segmented_data [4] :: STRING
            )
        ) AS currentTick,
        ABS(GREATEST(deltaQty0, deltaQty1)) AS amountOut,
        ABS(LEAST(deltaQty0, deltaQty1)) AS amountIn,
        token0,
        token1,
        CASE
            WHEN deltaQty0 < 0 THEN token0
            ELSE token1
        END AS token_in,
        CASE
            WHEN deltaQty0 > 0 THEN token0
            ELSE token1
        END AS token_out,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
        INNER JOIN pools p
        ON p.pool_address = l.contract_address
    WHERE
        topics [0] :: STRING = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67' -- elastic swap

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
    reipient_address AS tx_to,
    deltaQty0 AS delta_qty0,
    deltaQty1 AS delta_qty1,
    sqrtP AS sqrt_p,
    liquidity,
    currentTick AS current_tick,
    amountIn AS amount_in_unadj,
    amountOut AS amount_out_unadj,
    token0,
    token1,
    token_in,
    token_out,
    'Elastic Swap' AS event_name,
    'kyberswap-v2' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base
WHERE
    token_in <> token_out
