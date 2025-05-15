{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_bridge','defi','bridge','curated']
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
        'hop' AS NAME,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        TRY_TO_NUMBER(
            decoded_log :"amount" :: STRING
        ) AS amount,
        TRY_TO_NUMBER(
            decoded_log :"amountOutMin" :: STRING
        ) AS amountOutMin,
        TRY_TO_NUMBER(
            decoded_log :"chainId" :: STRING
        ) AS chainId,
        TRY_TO_TIMESTAMP(
            decoded_log :"deadline" :: STRING
        ) AS deadline,
        decoded_log :"recipient" :: STRING AS recipient,
        decoded_log :"relayer" :: STRING AS relayer,
        TRY_TO_NUMBER(
            decoded_log :"relayerFee" :: STRING
        ) AS relayerFee,
        decoded_log AS decoded_flat,
        event_removed,
        IFF(
            tx_succeeded,
            'SUCCESS',
            'FAIL'
        ) AS tx_status,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        topics [0] :: STRING = '0x0a0607688c86ec1775abcdbab7b33a3a35a6c9cde677c9be880150c231cc6b0b'
        AND origin_to_address IS NOT NULL
        AND tx_succeeded

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
hop_tokens AS (
    SELECT
        block_number,
        contract_address,
        token_address,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__hop_l1canonicaltoken') }}
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
    recipient AS receiver,
    receiver AS destination_chain_receiver,
    chainId AS destination_chain_id,
    token_address,
    amount,
    amountOutMin AS amount_out_min,
    deadline,
    relayer,
    relayerFee AS relayer_fee,
    _log_id,
    _inserted_timestamp
FROM
    base_evt b
    LEFT JOIN hop_tokens h USING(contract_address)
