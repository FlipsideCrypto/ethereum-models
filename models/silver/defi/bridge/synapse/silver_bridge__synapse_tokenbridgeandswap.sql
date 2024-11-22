{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH base_evt AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        'synapse' AS NAME,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        TRY_TO_NUMBER(
            decoded_flat :"amount" :: STRING
        ) AS amount,
        TRY_TO_NUMBER(
            decoded_flat :"chainId" :: STRING
        ) AS chainId,
        TRY_TO_TIMESTAMP(
            decoded_flat :"deadline" :: STRING
        ) AS deadline,
        TRY_TO_NUMBER(
            decoded_flat :"minDy" :: STRING
        ) AS minDy,
        decoded_flat :"to" :: STRING AS to_address,
        decoded_flat :"token" :: STRING AS token,
        TRY_TO_NUMBER(
            decoded_flat :"tokenIndexFrom" :: STRING
        ) AS tokenIndexFrom,
        TRY_TO_NUMBER(
            decoded_flat :"tokenIndexTo" :: STRING
        ) AS tokenIndexTo,
        decoded_flat,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] :: STRING = '0x79c15604b92ef54d3f61f0c40caab8857927ca3d5092367163b4562c1699eb5f'
        AND contract_address = '0x2796317b0ff8538f253012862c06787adfb8ceb6'
        AND tx_status = 'SUCCESS'

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
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    topic_0,
    event_name,
    event_removed,
    tx_status,
    contract_address AS bridge_address,
    NAME AS platform,
    origin_from_address AS sender,
    to_address AS receiver,
    receiver AS destination_chain_receiver,
    amount,
    chainId AS destination_chain_id,
    token AS token_address,
    deadline,
    minDy AS min_dy,
    tokenIndexFrom AS token_index_from,
    tokenIndexTo AS token_index_to,
    _log_id,
    _inserted_timestamp
FROM
    base_evt
