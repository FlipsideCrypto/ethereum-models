{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
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
name_registered AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        event_name,
        TRY_TO_NUMBER(
            decoded_flat :"cost" :: STRING
        ) AS cost_raw,
        cost_raw / pow(
            10,
            18
        ) AS cost_adj,
        decoded_flat :"expires" :: STRING AS expires,
        TRY_TO_TIMESTAMP(expires) AS expires_timestamp,
        decoded_flat :"label" :: STRING AS label,
        decoded_flat :"name" :: STRING AS NAME,
        decoded_flat :"owner" :: STRING AS owner,
        NULL AS premium_raw,
        NULL AS premium_adj,
        _log_id,
        _inserted_timestamp
    FROM
        base_events
    WHERE
        topic_0 = '0xca6abbe9d7f11422cb6ca7629fbf6fe9efb1c621f71ce8f02b9f2a230097404f' --v1
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        event_name,
        TRY_TO_NUMBER(
            decoded_flat :"baseCost" :: STRING
        ) AS cost_raw,
        cost_raw / pow(
            10,
            18
        ) AS cost_adj,
        decoded_flat :"expires" :: STRING AS expires,
        TRY_TO_TIMESTAMP(expires) AS expires_timestamp,
        decoded_flat :"label" :: STRING AS label,
        decoded_flat :"name" :: STRING AS NAME,
        decoded_flat :"owner" :: STRING AS owner,
        TRY_TO_NUMBER(
            decoded_flat :"premium" :: STRING
        ) AS premium_raw,
        premium_raw / pow(
            10,
            18
        ) AS premium_adj,
        _log_id,
        _inserted_timestamp
    FROM
        base_events
    WHERE
        topic_0 = '0x69e37f151eb98a09618ddaa80c8cfaf1ce5996867c489f45b555b412271ebf27' --v2
),

new_resolver AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        event_name,
        decoded_flat :"node" :: STRING AS node,
        decoded_flat :"resolver" :: STRING AS resolver,
        _log_id,
        _inserted_timestamp
    FROM
        base_events
    WHERE
        topic_0 = '0x335721b01866dc23fbee8b6b2c7b1e14d6f05c28cd35a2c934239f94095602a0'
)

SELECT
    n.block_number,
    n.block_timestamp,
    n.tx_hash,
    n.origin_function_signature,
    n.origin_from_address,
    n.origin_to_address,
    n.contract_address,
    n.event_index,
    n.event_name,
    n.origin_from_address AS manager,
    owner,
    NAME,
    label,
    node,
    resolver,
    cost_raw,
    cost_adj AS cost,
    premium_raw,
    premium_adj AS premium,
    expires,
    expires_timestamp,
    n._log_id,
    n._inserted_timestamp
FROM
    name_registered n
LEFT JOIN new_resolver r 
    ON n.block_number = r.block_number
    AND n.tx_hash = r.tx_hash
    AND n.event_index = ((r.event_index + 3) OR (r.event_index + 5))
