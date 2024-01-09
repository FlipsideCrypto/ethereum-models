{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH pools AS (

    SELECT
        pool_address
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
        --The trader wallet address that will swap with the contract. This can be a proxy contract
        decoded_flat :trader :: STRING AS trader_address,
        --The wallet address of the actual trader
        decoded_flat :effectiveTrader AS effective_trader_address,
        decoded_flat :txid AS txid,
        decoded_flat :baseToken AS tokenIn,
        decoded_flat :quoteToken AS tokenOut,
        TRY_TO_NUMBER(
            decoded_flat :baseTokenAmount :: STRING
        ) AS amountIn,
        TRY_TO_NUMBER(
            decoded_flat :quoteTokenAmount :: STRING
        ) AS amountOut,
        l._log_id,
        l._inserted_timestamp
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
        decoded_flat :dstChainId AS chain_id,
        contract_address,
        CASE
            WHEN chain_id = '20' THEN utils.udf_hex_to_base58(
                decoded_flat :dstTrader
            )
            ELSE LOWER(CONCAT('0x', SUBSTR(decoded_flat :dstTrader, 27)))
        END AS tx_to,
        --The trader wallet address that will swap with the contract. This can be a proxy contract
        decoded_flat :trader AS trader_address,
        --The wallet address of the actual trader
        decoded_flat :txid AS txid,
        decoded_flat :baseToken AS tokenIn,
        CASE
            WHEN chain_id = '20' THEN utils.udf_hex_to_base58(
                decoded_flat :quoteToken
            )
            ELSE LOWER(CONCAT('0x', SUBSTR(decoded_flat :quoteToken, 27)))
        END AS tokenOut,
        TRY_TO_NUMBER(
            decoded_flat :baseTokenAmount :: STRING
        ) AS amountIn,
        TRY_TO_NUMBER(
            decoded_flat :quoteTokenAmount :: STRING
        ) AS amountOut,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
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
