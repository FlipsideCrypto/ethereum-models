{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH pools AS (

    SELECT
        pool_address
    FROM
        {{ ref('silver_dex__hashflow_pools') }}
),

router_swaps_base AS (

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
        CONCAT('0x', SUBSTR(l_segmented_data [1] :: STRING, 25, 40)) AS account_address,
        CONCAT('0x', SUBSTR(l_segmented_data [3] :: STRING, 25, 40)) AS tokenIn,
        CONCAT('0x', SUBSTR(l_segmented_data [4] :: STRING, 25, 40)) AS tokenOut,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [5] :: STRING
            )
        ) AS amountIn,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [6] :: STRING
            )
        ) AS amountOut,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
        INNER JOIN pools p
        ON l.contract_address = p.pool_address
    WHERE
        l.topics [0] :: STRING = '0xb709ddcc6550418e9b89df1f4938071eeaa3f6376309904c77e15d46b16066f5' --swap

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
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
        CONCAT('0x', SUBSTR(l_segmented_data [0] :: STRING, 25, 40)) AS account_address,
        CONCAT('0x', SUBSTR(l_segmented_data [2] :: STRING, 25, 40)) AS tokenIn,
        CONCAT('0x', SUBSTR(l_segmented_data [3] :: STRING, 25, 40)) AS tokenOut,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [4] :: STRING
            )
        ) AS amountIn,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [5] :: STRING
            )
        ) AS amountOut,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
        INNER JOIN pools p
        ON l.contract_address = p.pool_address
    WHERE
        l.topics [0] :: STRING = '0x8cf3dec1929508e5677d7db003124e74802bfba7250a572205a9986d86ca9f1e' --swap

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
    origin_from_address AS sender,
    account_address AS tx_to,
    tokenIn AS token_in,
    tokenOut AS token_out,
    amountIn AS amount_in_unadj,
    amountOut AS amount_out_unadj,
    'Swap' AS event_name,
    'hashflow' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    router_swaps_base
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    event_index,
    contract_address,
    origin_from_address AS sender,
    account_address AS tx_to,
    tokenIn AS token_in,
    tokenOut AS token_out,
    amountIn AS amount_in_unadj,
    amountOut AS amount_out_unadj,
    'Swap' AS event_name,
    'hashflow' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base
