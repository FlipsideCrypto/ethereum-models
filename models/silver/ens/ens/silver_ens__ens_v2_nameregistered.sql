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
    'ens' AS NAME,
    event_index,
    topics [0] :: STRING AS topic_0,
    event_name,
    decoded_flat :"baseCost" :: STRING AS baseCost,
    decoded_flat :"expires" :: STRING AS expires,
    decoded_flat :"label" :: STRING AS label,
    decoded_flat :"name" :: STRING AS NAME,
    decoded_flat :"owner" :: STRING AS owner,
    decoded_flat :"premium" :: STRING AS premium,
    decoded_flat,
    event_removed,
    tx_status,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__decoded_logs') }}
WHERE
    topics [0] :: STRING = '0x69e37f151eb98a09618ddaa80c8cfaf1ce5996867c489f45b555b412271ebf27'
    AND contract_address = '0x253553366da8546fc250f225fe3d25d0c782303b'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
