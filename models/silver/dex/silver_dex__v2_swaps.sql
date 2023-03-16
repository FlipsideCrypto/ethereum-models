{{ config(
    materialized = 'incremental',
    persist_docs ={ "relation": true,
    "columns": true },
    unique_key = '_log_id',
    cluster_by = ['_inserted_timestamp::DATE']
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
        'Swap' AS event_name,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        PUBLIC.udf_hex_to_int(segmented_data[0]::STRING) :: INTEGER AS amount0In,
        PUBLIC.udf_hex_to_int(segmented_data[1]::STRING) :: INTEGER AS amount1In,
        PUBLIC.udf_hex_to_int(segmented_data[2]::STRING) :: INTEGER AS amount0Out,
        PUBLIC.udf_hex_to_int(segmented_data[3]::STRING) :: INTEGER AS amount1Out,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS sender,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS tx_to,
        event_index,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics[0] = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822'
        AND tx_status = 'SUCCESS'
        AND contract_address IN (
            SELECT
                DISTINCT pool_address
            FROM
                v2_pairs
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
)

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
    _inserted_timestamp,
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