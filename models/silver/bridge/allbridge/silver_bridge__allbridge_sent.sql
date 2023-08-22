{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    tags = ['non_realtime']
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
        'allbridge' AS NAME,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        TRY_TO_NUMBER(
            decoded_flat :"amount" :: STRING
        ) AS amount,
        utils.udf_hex_to_string(
            SUBSTRING(
                decoded_flat :"destination" :: STRING,
                3
            )
        ) AS destination_chain,
        decoded_flat :"lockId" :: STRING AS lockId,
        decoded_flat :"recipient" :: STRING AS recipient,
        decoded_flat :"sender" :: STRING AS sender,
        utils.udf_hex_to_string(
            SUBSTRING(
                decoded_flat :"tokenSource" :: STRING,
                3
            )
        ) AS token_source,
        REGEXP_REPLACE(
            decoded_flat :"tokenSourceAddress" :: STRING, '0+$', '') AS tokenSourceAddress,
        decoded_flat,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] :: STRING = '0x884a8def17f0d5bbb3fef53f3136b5320c9b39f75afb8985eeab9ea1153ee56d'
        AND contract_address = '0xbbbd1bbb4f9b936c3604906d7592a644071de884'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
HAVING LENGTH(tokenSourceAddress) = 42
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
    sender,
    recipient AS receiver,
    amount,
    lockId AS lock_id,
    destination_chain,
    token_source AS source_chain,
    tokenSourceAddress AS token_address,
    _log_id,
    _inserted_timestamp
FROM
    base_evt
