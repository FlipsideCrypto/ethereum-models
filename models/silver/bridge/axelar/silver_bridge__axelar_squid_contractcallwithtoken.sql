{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg']
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
        'axelar_squid' AS NAME,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        TRY_TO_NUMBER(
            decoded_flat :"amount" :: STRING
        ) AS amount,
        decoded_flat :"destinationChain" :: STRING AS destinationChain,
        LOWER(
            decoded_flat :"destinationContractAddress" :: STRING
        ) AS destinationContractAddress,
        decoded_flat :"payload" :: STRING AS payload,
        decoded_flat :"payloadHash" :: STRING AS payloadHash,
        decoded_flat :"sender" :: STRING AS sender,
        decoded_flat :"symbol" :: STRING AS symbol,
        decoded_flat,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] :: STRING = '0x7e50569d26be643bda7757722291ec66b1be66d8283474ae3fab5a98f878a7a2'
        AND contract_address = '0x4f4495243837681061c4743b74b3eedf548d56a5'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
transfers AS (
    SELECT
        block_number,
        tx_hash,
        event_index,
        contract_address AS token_address,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        from_address = '0xce16f69375520ab01377ce7b88f5ba8c48f8d666'
        AND to_address = '0x4f4495243837681061c4743b74b3eedf548d56a5'

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
    b.block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    b.tx_hash,
    b.event_index,
    topic_0,
    event_name,
    event_removed,
    tx_status,
    b.contract_address AS bridge_address,
    NAME AS platform,
    sender,
    sender AS receiver,
    destinationChain AS destination_chain,
    destinationContractAddress AS destination_contract_address,
    amount,
    payload,
    payloadHash AS payload_hash,
    symbol AS token_symbol,
    token_address,
    b._log_id,
    b._inserted_timestamp
FROM
    base_evt b
    JOIN transfers t
    ON b.block_number = t.block_number
    AND b.tx_hash = t.tx_hash
