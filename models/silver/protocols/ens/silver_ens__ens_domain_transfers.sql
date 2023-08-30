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
            '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
            '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62',
            '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb'
        )
        AND contract_address IN (
            '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85',
            '0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401'
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

transfers AS (
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
        NULL AS operator,
        decoded_flat :"from" :: STRING AS from_address,
        decoded_flat :"to" :: STRING AS to_address,
        decoded_flat :"tokenId" :: STRING AS token_id,
        NULL AS token_value,
        _log_id,
        _inserted_timestamp
    FROM
        base_events
    WHERE
        topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
),

transfers_single AS (
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
        decoded_flat :"from" :: STRING AS from_address,
        decoded_flat :"id" :: STRING AS token_id,
        decoded_flat :"operator" :: STRING AS OPERATOR,
        decoded_flat :"to" :: STRING AS to_address,
        decoded_flat :"value" :: STRING AS token_value,
        _log_id,
        _inserted_timestamp
    FROM
        base_events
    WHERE
        topic_0 = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
),

transfers_batch AS (
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
        decoded_flat :"from" :: STRING AS from_address,
        decoded_flat :"ids" :: STRING AS token_ids,
        VALUE :: STRING AS token_id,
        decoded_flat :"operator" :: STRING AS OPERATOR,
        decoded_flat :"to" :: STRING AS to_address,
        decoded_flat :"values" :: VARIANT AS token_values,
        token_values[0] :: STRING AS token_value,
        CONCAT(
            _log_id,
            '-',
            token_id
        ) AS _log_id,
        _inserted_timestamp
    FROM
        base_events,
        LATERAL FLATTEN (
            input => decoded_flat :ids
        )
    WHERE
        topic_0 = '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb'
)

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
    operator,
    from_address,
    to_address,
    token_id,
    token_value,
    _log_id,
    _inserted_timestamp
FROM transfers 
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
    operator,
    from_address,
    to_address,
    token_id,
    token_value,
    _log_id,
    _inserted_timestamp
FROM transfers_single
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
    operator,
    from_address,
    to_address,
    token_id,
    token_value,
    _log_id,
    _inserted_timestamp
FROM transfers_batch