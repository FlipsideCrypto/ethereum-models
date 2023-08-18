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
    'across' AS NAME,
    event_index,
    topics [0] :: STRING AS topic_0,
    event_name,
    decoded_flat :"amount" :: INTEGER AS amount,
    decoded_flat :"depositId" :: INTEGER AS depositId,
    decoded_flat :"depositor" :: STRING AS depositor,
    decoded_flat :"destinationChainId" :: INTEGER AS destinationChainId,
    decoded_flat :"message" :: STRING AS message,
    decoded_flat :"originChainId" :: INTEGER AS originChainId,
    decoded_flat :"originToken" :: STRING AS originToken,
    decoded_flat :"quoteTimestamp" :: INTEGER AS quoteTimestamp,
    decoded_flat :"recipient" :: STRING AS recipient,
    decoded_flat :"relayerFeePct" :: INTEGER AS relayerFeePct,
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

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
