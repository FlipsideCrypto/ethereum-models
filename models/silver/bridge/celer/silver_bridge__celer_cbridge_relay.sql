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
    decoded_flat :"receiver" :: STRING AS receiver,
    decoded_flat :"sender" :: STRING AS sender,
    decoded_flat :"srcChainId" :: INTEGER AS srcChainId,
    decoded_flat :"srcTransferId" :: STRING AS srcTransferId,
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
    topics [0] :: STRING = '0x79fa08de5149d912dce8e5e8da7a7c17ccdf23dd5d3bfe196802e6eb86347c7c'
    AND contract_address = '0x5427fefa711eff984124bfbb1ab6fbf5e3da1820'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
