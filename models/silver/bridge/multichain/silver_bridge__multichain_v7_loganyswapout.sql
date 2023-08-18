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
    decoded_flat :"from" :: STRING AS from_address,
    decoded_flat :"receiver" :: STRING AS receiver,
    decoded_flat :"swapoutID" :: STRING AS swapoutID,
    decoded_flat :"toChainID" :: INTEGER AS toChainID,
    decoded_flat :"token" :: STRING AS token,
    decoded_flat,
    event_removed,
    tx_status,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__decoded_logs') }}
WHERE
    topics [0] :: STRING = '0x0d969ae475ff6fcaf0dcfa760d4d8607244e8d95e9bf426f8d5d69f9a3e525af'
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
