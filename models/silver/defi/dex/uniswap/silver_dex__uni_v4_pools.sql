{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "pool_id",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','pools']
) }}

WITH initialize AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        topic_1 AS pool_id,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) AS currency0,
        CONCAT('0x', SUBSTR(topic_3, 27, 40)) AS currency1,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [0] :: STRING
            )
        ) AS fee,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [1] :: STRING
            )
        ) AS tick_spacing,
        CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 25, 40)) AS hook_address,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [3] :: STRING
        ) AS sqrtPriceX96,
        TRY_TO_DOUBLE(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [4] :: STRING
            )
        ) AS tick,
        CASE
            WHEN fee = 8388608 THEN TRUE
            ELSE FALSE
        END AS dynamic_fees,
        utils.udf_int_to_binary(
            utils.udf_hex_to_int(RIGHT(hook_address, 4))
        ) AS hook_flag_unsorted,
        CASE
            WHEN hook_flag_unsorted = '0' THEN '0000000000000000'
            ELSE hook_flag_unsorted
        END AS hook_flag,
        COALESCE(TRY_TO_BOOLEAN(SUBSTR(hook_flag, -14, 1)), FALSE) AS beforeInitialize,
        COALESCE(TRY_TO_BOOLEAN(SUBSTR(hook_flag, -13, 1)), FALSE) AS afterInitialize,
        COALESCE(TRY_TO_BOOLEAN(SUBSTR(hook_flag, -12, 1)), FALSE) AS beforeAddLiquidity,
        COALESCE(TRY_TO_BOOLEAN(SUBSTR(hook_flag, -11, 1)), FALSE) AS afterAddLiquidity,
        COALESCE(TRY_TO_BOOLEAN(SUBSTR(hook_flag, -10, 1)), FALSE) AS beforeRemoveLiquidity,
        COALESCE(TRY_TO_BOOLEAN(SUBSTR(hook_flag, -9, 1)), FALSE) AS afterRemoveLiquidity,
        COALESCE(TRY_TO_BOOLEAN(SUBSTR(hook_flag, -8, 1)), FALSE) AS beforeSwap,
        COALESCE(TRY_TO_BOOLEAN(SUBSTR(hook_flag, -7, 1)), FALSE) AS afterSwap,
        COALESCE(TRY_TO_BOOLEAN(SUBSTR(hook_flag, -6, 1)), FALSE) AS beforeDonate,
        COALESCE(TRY_TO_BOOLEAN(SUBSTR(hook_flag, -5, 1)), FALSE) AS afterDonate,
        COALESCE(TRY_TO_BOOLEAN(SUBSTR(hook_flag, -4, 1)), FALSE) AS beforeSwapReturnDelta,
        COALESCE(TRY_TO_BOOLEAN(SUBSTR(hook_flag, -3, 1)), FALSE) AS afterSwapReturnDelta,
        COALESCE(TRY_TO_BOOLEAN(SUBSTR(hook_flag, -2, 1)), FALSE) AS afterAddLiquidityReturnDelta,
        COALESCE(TRY_TO_BOOLEAN(SUBSTR(hook_flag, -1, 1)), FALSE) AS afterRemoveLiquidityReturnDelta,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2025-01-22'
        AND contract_address = '0x000000000004444c5dc75cb358380d2e3de08a90'
        AND topic_0 = '0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438' -- initialize
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
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    contract_address AS pool_address,
    CASE
        WHEN hook_address = '0x0000000000000000000000000000000000000000' THEN CONCAT(COALESCE(fee, 0), ' ', COALESCE(tick_spacing, 0))
        ELSE CONCAT(COALESCE(fee, 0), ' ', COALESCE(tick_spacing, 0), ' ', hook_address)
    END AS pool_name,
    pool_id,
    currency0 AS token0,
    currency1 AS token1,
    fee,
    tick_spacing,
    hook_address,
    sqrtPriceX96,
    tick,
    dynamic_fees,
    beforeInitialize,
    afterInitialize,
    beforeAddLiquidity,
    afterAddLiquidity,
    beforeRemoveLiquidity,
    afterRemoveLiquidity,
    beforeSwap,
    afterSwap,
    beforeDonate,
    afterDonate,
    beforeSwapReturnDelta,
    afterSwapReturnDelta,
    afterAddLiquidityReturnDelta,
    'uniswap-v4' AS platform,
    'v4' AS version,
    _log_id,
    _inserted_timestamp
FROM
    initialize qualify (ROW_NUMBER() over (PARTITION BY pool_id
ORDER BY
    _inserted_timestamp DESC)) = 1
