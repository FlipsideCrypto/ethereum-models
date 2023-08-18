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
    'celer_cbridge' AS NAME,
    event_index,
    topics [0] :: STRING AS topic_0,
    event_name,
    decoded_flat :"amount" :: INTEGER AS amount,
    decoded_flat :"dstChainId" :: INTEGER AS dstChainId,
    decoded_flat :"maxSlippage" :: INTEGER AS maxSlippage,
    decoded_flat :"nonce" :: INTEGER AS nonce,
    decoded_flat :"receiver" :: STRING AS receiver,
    decoded_flat :"sender" :: STRING AS sender,
    decoded_flat :"token" :: STRING AS token,
    decoded_flat :"transferId" :: STRING AS transferId,
    decoded_flat,
    event_removed,
    tx_status,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__decoded_logs') }}
WHERE
    topics [0] :: STRING = '0x89d8051e597ab4178a863a5190407b98abfeff406aa8db90c59af76612e58f01'
    AND contract_address = '0x5427fefa711eff984124bfbb1ab6fbf5e3da1820'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
