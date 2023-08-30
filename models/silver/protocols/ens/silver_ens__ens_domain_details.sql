{{ config(
    materialized = 'incremental',
    unique_key = '_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime']
) }}

WITH base_events AS (

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
        decoded_flat,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0xca6abbe9d7f11422cb6ca7629fbf6fe9efb1c621f71ce8f02b9f2a230097404f',
            '0x69e37f151eb98a09618ddaa80c8cfaf1ce5996867c489f45b555b412271ebf27',
            '0x335721b01866dc23fbee8b6b2c7b1e14d6f05c28cd35a2c934239f94095602a0'
        )
        AND contract_address IN (
            '0x283af0b28c62c092c9727f1ee09c02ca627eb7f5',
            '0x253553366da8546fc250f225fe3d25d0c782303b',
            '0x314159265dd8dbb310642f98f50c066173c1259b',
            '0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),