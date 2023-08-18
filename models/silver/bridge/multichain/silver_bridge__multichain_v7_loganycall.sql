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
    decoded_flat :"appID" :: STRING AS appID,
    decoded_flat :"data" :: STRING AS DATA,
    decoded_flat :"extdata" :: STRING AS extdata,
    decoded_flat :"flags" :: INTEGER AS flags,
    decoded_flat :"from" :: STRING AS from_address,
    decoded_flat :"nonce" :: INTEGER AS nonce,
    decoded_flat :"to" :: STRING AS to_address,
    decoded_flat :"toChainID" :: INTEGER AS toChainID,
    decoded_flat,
    event_removed,
    tx_status,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__decoded_logs') }}
WHERE
    topics [0] :: STRING = '0x17dac14bf31c4070ebb2dc182fc25ae5df58f14162a7f24a65b103e22385af0d'
    AND contract_address = '0x8efd012977dd5c97e959b9e48c04ee5fcd604374'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
