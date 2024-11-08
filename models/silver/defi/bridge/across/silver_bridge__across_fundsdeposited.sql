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
            decoded_log :"amount" :: STRING
        ) AS amount,
        TRY_TO_NUMBER(
            decoded_log :"depositId" :: STRING
        ) AS depositId,
        decoded_log :"depositor" :: STRING AS depositor,
        TRY_TO_NUMBER(
            decoded_log :"destinationChainId" :: STRING
        ) AS destinationChainId,
        decoded_log :"message" :: STRING AS message,
        TRY_TO_NUMBER(
            decoded_log :"originChainId" :: STRING
        ) AS originChainId,
        decoded_log :"originToken" :: STRING AS originToken,
        TRY_TO_TIMESTAMP(
            decoded_log :"quoteTimestamp" :: STRING
        ) AS quoteTimestamp,
        decoded_log :"recipient" :: STRING AS recipient,
        TRY_TO_NUMBER(
            decoded_log :"relayerFeePct" :: STRING
        ) AS relayerFeePct,
        decoded_log AS decoded_flat,
        event_removed,
        tx_succeeded,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        topics [0] :: STRING = '0xafc4df6845a4ab948b492800d3d8a25d538a102a2bc07cd01f1cfa097fddcff6'
        AND contract_address = '0x5c7bcd6e7de5423a257d81b442095a1a6ced35c5'
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
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
            decoded_log :"amount" :: STRING
        ) AS amount,
        TRY_TO_NUMBER(
            decoded_log :"dstChainId" :: STRING
        ) AS dstChainId,
        decoded_log :"dstToken" :: STRING AS dstToken,
        decoded_log :"from" :: STRING AS from_address,
        decoded_log :"to" :: STRING AS to_address,
        decoded_log AS decoded_flat,
        event_removed,
        tx_succeeded,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        topics [0] :: STRING = '0x0cf77fd2585a4d672259e86a6adb2f6b05334cbb420727afcfbc689d018bb456'
        AND contract_address = '0x1a9f622dfafad5373741d821f1431abb23c30529'
        AND tx_succeeded

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
    A.tx_succeeded,
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
