{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    tags = ['non_realtime']
) }}

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
    decoded_flat :"amount" :: INTEGER AS amount,
    decoded_flat :"lockId" :: INTEGER AS lockId,
    decoded_flat :"recipient" :: STRING AS recipient,
    decoded_flat :"source" :: STRING AS source,
    decoded_flat :"token" :: STRING AS token,
    decoded_flat,
    event_removed,
    tx_status,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__decoded_logs') }}
WHERE
    topics [0] :: STRING = '0xeeff8dc309b75f785752dd67594b2d8a3a9fd4ff6ecd65fcfe670cee0d851ce4'
    AND contract_address = '0xbbbd1bbb4f9b936c3604906d7592a644071de884'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
