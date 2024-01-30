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
        'eywa' AS NAME,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        decoded_flat,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0x5566d73d091d945ab32ea023cd1930c0d43aa43bef9aee4cb029775cfc94bdae',
            --RequestSent
            '0xb5f411fa3c897c9b0b6cd61852278a67e73d885610724a5610a8580d3e94cfdb'
        ) --locked
        AND contract_address IN (
            '0xece9cf6a8f2768a3b8b65060925b646afeaa5167',
            --BridgeV2
            '0xac8f44ceca92b2a4b30360e5bd3043850a0ffcbe',
            --PortalV2
            '0xbf0b5d561b986809924f88099c4ff0e6bcce60c9' --PortalV2
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
requestsent AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        NAME,
        event_index,
        topic_0,
        event_name,
        decoded_flat :"chainIdTo" :: STRING AS chainIdTo,
        decoded_flat :"data" :: STRING AS data_requestsent,
        decoded_flat :"requestId" :: STRING AS requestId,
        decoded_flat :"to" :: STRING AS to_address,
        decoded_flat,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM
        base_evt
    WHERE
        topic_0 = '0x5566d73d091d945ab32ea023cd1930c0d43aa43bef9aee4cb029775cfc94bdae' --RequestSent
),
locked AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        NAME,
        event_index,
        topic_0,
        event_name,
        TRY_TO_NUMBER(
            decoded_flat :"amount" :: STRING
        ) AS amount,
        decoded_flat :"from" :: STRING AS from_address,
        decoded_flat :"to" :: STRING AS to_address,
        decoded_flat :"token" :: STRING AS token,
        decoded_flat,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM
        base_evt
    WHERE
        topic_0 = '0xb5f411fa3c897c9b0b6cd61852278a67e73d885610724a5610a8580d3e94cfdb' --Locked
)
SELECT
    r.block_number,
    r.block_timestamp,
    r.origin_function_signature,
    r.origin_from_address,
    r.origin_to_address,
    r.tx_hash,
    r.event_index,
    r.topic_0,
    r.event_name,
    r.event_removed,
    r.tx_status,
    r.contract_address AS bridge_address,
    r.name AS platform,
    l.from_address AS sender,
    sender AS receiver,
    receiver AS destination_chain_receiver,
    l.amount,
    r.chainIdTo AS destination_chain_id,
    l.token AS token_address,
    _log_id,
    _inserted_timestamp
FROM
    requestsent r
    LEFT JOIN locked l USING(
        block_number,
        tx_hash
    )
WHERE token_address IS NOT NULL
