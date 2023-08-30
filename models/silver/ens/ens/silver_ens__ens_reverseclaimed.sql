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
    decoded_flat :"addr" :: STRING AS addr,
    decoded_flat :"node" :: STRING AS node,
    decoded_flat,
    event_removed,
    tx_status,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__decoded_logs') }}
WHERE
    topics [0] :: STRING = '0x6ada868dd3058cf77a48a74489fd7963688e5464b2b0fa957ace976243270e92'
    AND contract_address = '0xa58e81fe9b61b5c3fe2afd33cf304c454abfc7cb'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
--requires txns/traces CTE based on input data for other contracts
{# WHERE
        origin_function_signature = '0xc47f0027'
        AND to_address IN (
     {# 0x9062c0a6dbd6108336bcbe4593a3d1ce05512069 -- ENS: Old Reverse Registrar
            , 0x084b1c3c81545d370f3634392de611caabff8148 -- ENS: Old Reverse Registrar 2
            , 0xa58e81fe9b61b5c3fe2afd33cf304c454abfc7cb -- ENS: Reverse Registrar #}
            )