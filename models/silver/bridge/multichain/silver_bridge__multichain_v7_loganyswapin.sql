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
    'multichain' AS NAME,
    event_index,
    topics [0] :: STRING AS topic_0,
    event_name,
    decoded_flat :"amount" :: INTEGER AS amount,
    decoded_flat :"fromChainID" :: INTEGER AS fromChainID,
    decoded_flat :"receiver" :: STRING AS receiver,
    decoded_flat :"swapID" :: STRING AS swapID,
    decoded_flat :"swapoutID" :: STRING AS swapoutID,
    decoded_flat :"token" :: STRING AS token,
    decoded_flat,
    event_removed,
    tx_status,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__decoded_logs') }}
WHERE
    topics [0] :: STRING = '0x164f647883b52834be7a5219336e455a23a358be27519d0442fc0ee5e1b1ce2e'
    AND contract_address IN (
        '0x1633d66ca91ce4d81f63ea047b7b19beb92df7f3',
        '0x93251f98acb0c83904320737aec091bce287f8f5'
    )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
