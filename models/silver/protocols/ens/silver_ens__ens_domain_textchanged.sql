{{ config(
    materialized = 'incremental',
    unique_key = 'node',
    incremental_strategy = 'delete+insert',
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
        decoded_flat :"indexedKey" :: STRING AS indexedKey,
        decoded_flat :"key" :: STRING AS key,
        decoded_flat :"node" :: STRING AS node,
        decoded_flat,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] :: STRING = '0xd8c9334b1a9c2f9da342a0a2b32629c1a229b6445dad78947f674b44444a7550'
        AND contract_address = '0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '24 hours'
    FROM
        {{ this }}
)
{% endif %}
),
base_input_data AS (
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
        from_address,
        to_address,
        input_data,
        indexedKey,
        key,
        node,
        HEX_ENCODE(
            key :: STRING
        ) AS key_encoded,
        CHARINDEX(LOWER(key_encoded), input_data) AS key_index,
        CASE
            WHEN key_index = 0 THEN NULL
            ELSE SUBSTR(
                input_data,
                key_index + 128,
                128
            )
        END AS key_name_part,
        utils.udf_hex_to_string(
            key_name_part :: STRING
        ) AS key_translate,
        _log_id,
        _inserted_timestamp
    FROM
        base_events b
        LEFT JOIN {{ ref('silver__transactions') }}
        t USING(tx_hash) qualify(ROW_NUMBER() over (PARTITION BY node, key
    ORDER BY
        block_timestamp DESC)) = 1
)
SELECT
    MAX(block_number) AS latest_block,
    MAX(block_timestamp) AS latest_timestamp,
    ARRAY_AGG(tx_hash) AS tx_hash_array,
    node,
    OBJECT_AGG(
        key :: variant,
        key_translate :: variant
    ) AS profile_info,
    MAX(_inserted_timestamp) AS _inserted_timestamp
FROM
    base_input_data
GROUP BY
    node
--verify delete+insert works as expected, e.g. if new email comes in, 
--does it add/update the email in the object or does it delete every profile record except for the email