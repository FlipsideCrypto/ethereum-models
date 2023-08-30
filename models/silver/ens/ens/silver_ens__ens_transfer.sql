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
    decoded_flat :"from" :: STRING AS from_address,
    decoded_flat :"to" :: STRING AS to_address,
    decoded_flat :"tokenId" :: STRING AS tokenId,
    decoded_flat,
    event_removed,
    tx_status,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__decoded_logs') }}
WHERE
    topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND contract_address = '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
