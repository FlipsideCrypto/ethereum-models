{{ config(
    materialized = 'incremental',
    unique_key = 'node',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['silver','curated','ens']
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
        decoded_log :"indexedKey" :: STRING AS indexedKey,
        decoded_log :"key" :: STRING AS key,
        decoded_log :"node" :: STRING AS node,
        decoded_log AS decoded_flat,
        event_removed,
        tx_succeeded,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        topics [0] :: STRING = '0xd8c9334b1a9c2f9da342a0a2b32629c1a229b6445dad78947f674b44444a7550'
        AND contract_address = '0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
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
        LEFT JOIN {{ ref('core__fact_transactions') }}
        t USING(tx_hash) qualify(ROW_NUMBER() over (PARTITION BY node, key
    ORDER BY
        block_timestamp DESC)) = 1
),
aggregated_object AS (
    SELECT
        MAX(block_number) AS latest_block,
        MAX(block_timestamp) AS latest_timestamp,
        MAX(manager) AS manager,
        node,
        OBJECT_AGG(
            key :: variant,
            key_translate :: variant
        ) AS profile_info,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        base_input_data 
    INNER JOIN (
        SELECT node, origin_from_address AS manager, MAX(block_timestamp) AS latest_timestamp
        FROM base_input_data
        GROUP BY 1,2
        QUALIFY(ROW_NUMBER() OVER (PARTITION BY node ORDER BY latest_timestamp DESC)) = 1
    ) USING(node)
    GROUP BY
        node
)

{% if is_incremental() %},
merged AS (
    SELECT
        latest_block,
        latest_timestamp,
        manager,
        node,
        profile_info,
        _inserted_timestamp
    FROM
        aggregated_object
    UNION ALL
    SELECT
        latest_block,
        latest_timestamp,
        manager,
        node,
        profile_info,
        _inserted_timestamp
    FROM
        {{ this }}
),
flattened AS (
    SELECT
        latest_block,
        latest_timestamp,
        manager,
        node,
        profile_info,
        key,
        VALUE :: STRING AS VALUE,
        _inserted_timestamp
    FROM
        merged,
        LATERAL FLATTEN(input => profile_info) qualify(ROW_NUMBER() over (PARTITION BY node, key
    ORDER BY
        latest_timestamp DESC)) = 1
),
FINAL AS (
    SELECT
        MAX(latest_block) AS latest_block,
        MAX(latest_timestamp) AS latest_timestamp,
        MAX(manager) AS manager,
        node,
        OBJECT_AGG(
            key :: variant,
            VALUE :: variant
        ) AS profile_info,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        flattened
    GROUP BY
        node
)
SELECT
    latest_block,
    latest_timestamp,
    manager,
    node,
    profile_info,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['node']
    ) }} AS ens_domain_textchanged_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
{% else %}
SELECT
    latest_block,
    latest_timestamp,
    manager,
    node,
    profile_info,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['node']
    ) }} AS ens_domain_textchanged_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    aggregated_object
{% endif %}
