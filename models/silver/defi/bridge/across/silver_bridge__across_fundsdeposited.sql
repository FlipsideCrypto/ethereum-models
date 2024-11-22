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
        'across' AS NAME,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        TRY_TO_NUMBER(
            decoded_flat :"amount" :: STRING
        ) AS amount,
        TRY_TO_NUMBER(
            decoded_flat :"depositId" :: STRING
        ) AS depositId,
        decoded_flat :"depositor" :: STRING AS depositor,
        TRY_TO_NUMBER(
            decoded_flat :"destinationChainId" :: STRING
        ) AS destinationChainId,
        decoded_flat :"message" :: STRING AS message,
        TRY_TO_NUMBER(
            decoded_flat :"originChainId" :: STRING
        ) AS originChainId,
        decoded_flat :"originToken" :: STRING AS originToken,
        TRY_TO_TIMESTAMP(
            decoded_flat :"quoteTimestamp" :: STRING
        ) AS quoteTimestamp,
        decoded_flat :"recipient" :: STRING AS recipient,
        TRY_TO_NUMBER(
            decoded_flat :"relayerFeePct" :: STRING
        ) AS relayerFeePct,
        decoded_flat,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] :: STRING = '0xafc4df6845a4ab948b492800d3d8a25d538a102a2bc07cd01f1cfa097fddcff6'
        AND contract_address = '0x5c7bcd6e7de5423a257d81b442095a1a6ced35c5'
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
),
bridge_to AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        'across' AS NAME,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        TRY_TO_NUMBER(
            decoded_flat :"amount" :: STRING
        ) AS amount,
        TRY_TO_NUMBER(
            decoded_flat :"dstChainId" :: STRING
        ) AS dstChainId,
        decoded_flat :"dstToken" :: STRING AS dstToken,
        decoded_flat :"from" :: STRING AS from_address,
        decoded_flat :"to" :: STRING AS to_address,
        decoded_flat,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] :: STRING = '0x0cf77fd2585a4d672259e86a6adb2f6b05334cbb420727afcfbc689d018bb456'
        AND contract_address = '0x1a9f622dfafad5373741d821f1431abb23c30529'
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
    A.block_number,
    A.block_timestamp,
    A.origin_function_signature,
    A.origin_from_address,
    A.origin_to_address,
    A.tx_hash,
    A.event_index,
    A.topic_0,
    A.event_name,
    A.event_removed,
    A.tx_status,
    A.contract_address AS bridge_address,
    A.name AS platform,
    A.depositor AS sender,
    A.recipient AS receiver,
    CASE
        WHEN b.tx_hash IS NOT NULL THEN b.to_address
        ELSE A.recipient
    END AS destination_chain_receiver,
    A.destinationChainId AS destination_chain_id,
    A.amount,
    A.depositId AS deposit_id,
    A.message,
    A.originChainId AS origin_chain_id,
    A.originToken AS token_address,
    A.quoteTimestamp AS quote_timestamp,
    A.relayerFeePct AS relayer_fee_pct,
    A._log_id,
    A._inserted_timestamp
FROM
    base_evt A
    LEFT JOIN bridge_to b
    ON A.block_number = b.block_number
    AND A.tx_hash = b.tx_hash
