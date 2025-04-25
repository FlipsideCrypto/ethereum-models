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
        topic_1 AS id,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) AS sender,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_DOUBLE(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [0] :: STRING
            )
        ) AS amount0,
        TRY_TO_DOUBLE(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [1] :: STRING
            )
        ) AS amount1,
        TRY_TO_DOUBLE(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [2] :: STRING
            )
        ) AS sqrtPriceX96,
        TRY_TO_DOUBLE(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [3] :: STRING
            )
        ) AS liquidity,
        TRY_TO_DOUBLE(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [4] :: STRING
            )
        ) AS tick,
        TRY_TO_DOUBLE(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [4] :: STRING
            )
        ) AS fee,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = '0x000000000004444c5dc75cb358380d2e3de08a90'
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
        currency0,
        currency1,
        fee,
        id,
        tick_spacing,
        pool_address
    FROM
        {{ ref('silver_dex__uni_v4_pools') }}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    s.contract_address,
    pool_address,
    event_index,
    s.id,
    sender,
    sender as recipient,
    amount0 AS amount0_unadj,
    amount1 AS amount1_unadj,
    sqrtPriceX96,
    liquidity,
    tick,
    s.fee,
    currency0 as token0_address,
    currency1 as token1_address,
    tick_spacing,
    CONCAT(
        tx_hash :: STRING,
        '-',
        event_index :: STRING
    ) AS _log_id,
    _inserted_timestamp
FROM
    base_swaps s
    INNER JOIN pool_data p
    ON s.id = p.id
