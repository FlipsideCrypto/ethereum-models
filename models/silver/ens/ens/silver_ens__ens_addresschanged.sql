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
    event_index,
    topics [0] :: STRING AS topic_0,
    event_name,
    decoded_flat :"coinType" :: STRING AS coinType,
    decoded_flat :"newAddress" :: STRING AS newAddress,
    decoded_flat :"node" :: STRING AS node,
    decoded_flat,
    event_removed,
    tx_status,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__decoded_logs') }}
WHERE
    topics [0] :: STRING = '0x65412581168e88a1e60c6459d7f44ae83ad0832e670826c05a4e2476b57af752'
    AND contract_address = '0x231b0ee14048e9dccd1d247744d114a4eb5e8e63'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
