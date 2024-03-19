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
        'across-v3' AS NAME,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        TRY_TO_NUMBER(
            decoded_flat :"depositId" :: STRING
        ) AS depositId,
        decoded_flat :"depositor" :: STRING AS depositor,
        TRY_TO_NUMBER(
            decoded_flat :"destinationChainId" :: STRING
        ) AS destinationChainId,
        decoded_flat :"message" :: STRING AS message,
        TRY_TO_TIMESTAMP(
            decoded_flat :"quoteTimestamp" :: STRING
        ) AS quoteTimestamp,
        decoded_flat :"recipient" :: STRING AS recipient,
        TRY_TO_NUMBER(
            decoded_flat :"relayerFeePct" :: STRING
        ) AS relayerFeePct,
        decoded_flat :"exclusiveRelayer" :: STRING AS exclusiveRelayer,
        TRY_TO_NUMBER(
            decoded_flat :"exclusivityDeadline" :: STRING
        ) AS exclusivityDeadline,
        TRY_TO_NUMBER(
            decoded_flat :"fillDeadline" :: STRING
        ) AS fillDeadline,
        TRY_TO_NUMBER(
            decoded_flat :"inputAmount" :: STRING
        ) AS inputAmount,
        decoded_flat :"inputToken" :: STRING AS inputToken,
        TRY_TO_NUMBER(
            decoded_flat :"outputAmount" :: STRING
        ) AS outputAmount,
        decoded_flat :"outputToken" :: STRING AS outputToken,
        decoded_flat,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] :: STRING = '0xa123dc29aebf7d0c3322c8eeb5b999e859f39937950ed31056532713d0de396f'
        AND contract_address = '0x5c7bcd6e7de5423a257d81b442095a1a6ced35c5'

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
    contract_address AS bridge_address,
    NAME AS platform,
    depositor AS sender,
    recipient AS receiver,
    recipient AS destination_chain_receiver,
    destinationChainId AS destination_chain_id,
    inputAmount AS amount,
    inputToken AS token_address,
    depositId AS deposit_id,
    message,
    quoteTimestamp AS quote_timestamp,
    relayerFeePct AS relayer_fee_pct,
    exclusiveRelayer AS exclusive_relayer,
    exclusivityDeadline AS exclusivity_deadline,
    fillDeadline AS fill_deadline,
    outputAmount AS output_amount,
    outputToken AS output_token,
    _log_id,
    _inserted_timestamp
FROM
    base_evt
