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
    decoded_flat :"proposalId" :: STRING AS proposalId,
    decoded_flat :"reason" :: STRING AS reason,
    decoded_flat :"support" :: STRING AS support,
    decoded_flat :"voter" :: STRING AS voter,
    decoded_flat :"weight" :: STRING AS weight,
    decoded_flat,
    event_removed,
    tx_status,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__decoded_logs') }}
WHERE
    topics [0] :: STRING = '0xb8e138887d0aa13bab447e82de9d5c1777041ecd21ca36ba824ff1e6c07ddda4'
    AND contract_address = '0x323a76393544d5ecca80cd6ef2a560c6a395b7e3'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
