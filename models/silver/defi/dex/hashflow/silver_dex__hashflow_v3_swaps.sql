{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH pools AS (

    SELECT
        pool_address,
        pool_name
    FROM
        {{ ref('silver_dex__hashflow_v3_pools') }}
),
swaps AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.origin_function_signature,
        l.origin_from_address,
        l.origin_to_address,
        l.event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        p.pool_name,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS trader_address,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS effective_trader_address,
        CONCAT(
            '0x',
            segmented_data [2] :: STRING
        ) AS txid,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS tokenIn,
        CONCAT('0x', SUBSTR(segmented_data [4] :: STRING, 25, 40)) AS tokenOut,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [5] :: STRING
            )
        ) AS amountIn,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [6] :: STRING
            )
        ) AS amountOut,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
        l
        INNER JOIN pools p
        ON l.contract_address = p.pool_address
    WHERE
        l.topics [0] :: STRING = '0x34f57786fb01682fb4eec88d340387ef01a168fe345ea5b76f709d4e560c10eb' --Trade

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
router_swaps AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.origin_function_signature,
        l.origin_from_address,
        l.origin_to_address,
        'XChainTrade' AS event_name,
        l.event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        'HashflowRouter' AS pool_name,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS chain_id,
        CASE
            --ID 20 = Solana XChainTrade, address is decoded to Solana format
            WHEN chain_id = '20' THEN utils.udf_hex_to_base58(
                CONCAT(
                    '0x',
                    segmented_data [3] :: STRING
                )
            )
            ELSE CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 27, 40))
        END AS tx_to,
        --The trader wallet address that will swap with the contract. This can be a proxy contract
        CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 25, 40)) AS trader_address,
        --The wallet address of the actual trader
        segmented_data [4] :: STRING AS txid,
        CONCAT('0x', SUBSTR(segmented_data [5] :: STRING, 25, 40)) AS tokenIn,
        segmented_data [6] :: STRING AS raw_token_out,
        CASE
            WHEN chain_id = '20' THEN utils.udf_hex_to_base58(
                CONCAT(
                    '0x',
                    segmented_data [6] :: STRING
                )
            )
            ELSE CONCAT('0x', SUBSTR(segmented_data [6] :: STRING, 27, 40))
        END AS tokenOut,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [7] :: STRING
            )
        ) AS amountIn,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [8] :: STRING
            )
        ) AS amountOut,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        l.topics [0] :: STRING = '0x3f72b2a38919490277652bb34955c871b20e23068c243319c9fa5e27963d9e12' --Xchaintrade
        AND origin_to_address = LOWER('0x55084eE0fEf03f14a305cd24286359A35D735151') --router

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
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
    pool_name,
    effective_trader_address AS sender,
    trader_address AS tx_to,
    txid,
    CASE
        WHEN tokenIn = '0x0000000000000000000000000000000000000000' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE tokenIn
    END AS token_in,
    CASE
        WHEN tokenOut = '0x0000000000000000000000000000000000000000' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE tokenOut
    END AS token_out,
    amountIn AS amount_in_unadj,
    amountOut AS amount_out_unadj,
    'Trade' AS event_name,
    'hashflow-v3' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps
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
    pool_name,
    trader_address AS sender,
    tx_to,
    txid,
    CASE
        WHEN tokenIn = '0x0000000000000000000000000000000000000000' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE tokenIn
    END AS token_in,
    CASE
        WHEN tokenOut = '0x0000000000000000000000000000000000000000' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE tokenOut
    END AS token_out,
    amountIn AS amount_in_unadj,
    amountOut AS amount_out_unadj,
    event_name,
    'hashflow-v3' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    router_swaps
