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
    'stargate' AS NAME,
    event_index,
    topics [0] :: STRING AS topic_0,
    event_name,
    decoded_flat :"amountSD" :: INTEGER AS amountSD,
    decoded_flat :"chainId" :: INTEGER AS chainId,
    decoded_flat :"dstPoolId" :: INTEGER AS dstPoolId,
    decoded_flat :"eqFee" :: INTEGER AS eqFee,
    decoded_flat :"eqReward" :: INTEGER AS eqReward,
    decoded_flat :"from" :: STRING AS from_address,
    decoded_flat :"lpFee" :: INTEGER AS lpFee,
    decoded_flat :"protocolFee" :: INTEGER AS protocolFee,
    decoded_flat,
    event_removed,
    tx_status,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__decoded_logs') }}
WHERE
    topics [0] :: STRING = '0x34660fc8af304464529f48a778e03d03e4d34bcd5f9b6f0cfbf3cd238c642f7f'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
