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
        LOWER(token_address) AS token_address
    FROM
        {{ ref('silver_bridge__stargate_createpool') }}
),
base_evt AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        'stargate' AS NAME,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        TRY_TO_NUMBER(
            decoded_flat :"amountSD" :: STRING
        ) AS amountSD,
        TRY_TO_NUMBER(
            decoded_flat :"chainId" :: STRING
        ) AS chainId,
        CASE
            WHEN chainId < 100 THEN chainId + 100
            ELSE chainId
        END AS destination_chain_id,
        TRY_TO_NUMBER(
            decoded_flat :"dstPoolId" :: STRING
        ) AS dstPoolId,
        TRY_TO_NUMBER(
            decoded_flat :"eqFee" :: STRING
        ) AS eqFee,
        TRY_TO_NUMBER(
            decoded_flat :"eqReward" :: STRING
        ) AS eqReward,
        TRY_TO_NUMBER(
            decoded_flat :"amountSD" :: STRING
        ) AS lpFee,
        TRY_TO_NUMBER(
            decoded_flat :"amountSD" :: STRING
        ) AS protocolFee,
        decoded_flat :"from" :: STRING AS from_address,
        decoded_flat,
        token_address,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
        d
        INNER JOIN pools p
        ON d.contract_address = p.pool_address
    WHERE
        topics [0] :: STRING = '0x34660fc8af304464529f48a778e03d03e4d34bcd5f9b6f0cfbf3cd238c642f7f'

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
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    topic_0,
    event_name,
    event_removed,
    tx_status,
    '0x296f55f8fb28e498b858d0bcda06d955b2cb3f97' AS bridge_address,
    NAME AS platform,
    from_address AS sender,
    from_address AS receiver,
    amountSD AS amount_unadj,
    destination_chain_id,
    LOWER(chain_name) AS destination_chain,
    dstPoolId AS destination_pool_id,
    eqFee AS fee,
    eqReward AS reward,
    lpFee AS lp_fee,
    protocolFee AS protocol_fee,
    token_address,
    _log_id,
    _inserted_timestamp
FROM
    base_evt b
    LEFT JOIN {{ ref('silver_bridge__stargate_chain_id_seed') }}
    s
    ON b.destination_chain_id :: STRING = s.chain_id :: STRING
