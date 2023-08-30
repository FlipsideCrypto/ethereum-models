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
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_status,
        contract_address,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        topics,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address = '0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401' --NameWrapper
        AND topics [0] :: STRING IN (
            '0x8ce7013e8abebc55c3890a68f5a27c67c3f7efa64e584de5fb22363c606fd340',
            --NameWrapped
            '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62',
            --TransferSingle
            '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb' --TransferBatch
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
name_wrapped AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        topics [1] :: STRING AS node,
        utils.udf_hex_to_string(
            segmented_data [5] :: STRING
        ) AS NAME,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS owner,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) AS fuses,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) AS expiry,
        TRY_TO_TIMESTAMP(expiry) AS expiry_timestamp,
        _log_id,
        _inserted_timestamp
    FROM
        base_events
    WHERE
        topics [0] :: STRING = '0x8ce7013e8abebc55c3890a68f5a27c67c3f7efa64e584de5fb22363c606fd340'
),
transfer_single AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS OPERATOR,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS from_address,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS to_address,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS token_id,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS token_value,
        _log_id,
        _inserted_timestamp
    FROM
        base_events
    WHERE
        topics [0] :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
),
transfer_batch AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS OPERATOR,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS from_address,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS to_address,
        VALUE :: STRING AS token_id,
        1 AS token_value,
        CONCAT(
            _log_id,
            '-',
            token_id
        ) AS _log_id,
        _inserted_timestamp
    FROM
        base_events,
        LATERAL FLATTEN (
            input => segmented_data
        )
    WHERE
        topics [0] :: STRING = '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb'
        AND VALUE :: STRING NOT ILIKE '000000000000000000000000000000000000000000000000000000000%'
)
SELECT
    w.block_number,
    w.block_timestamp,
    w.tx_hash,
    w.event_index,
    w.origin_from_address,
    w.origin_to_address,
    w.origin_function_signature,
    w.contract_address,
    node,
    NAME,
    owner,
    fuses,
    expiry,
    expiry_timestamp,
    COALESCE(
        s.operator,
        b.operator
    ) AS operator_address,
    COALESCE(
        s.from_address,
        b.from_address
    ) AS from_address,
    COALESCE(
        s.to_address,
        b.to_address
    ) AS to_address,
    COALESCE(
        s.token_id,
        b.token_id
    ) AS token_id,
    COALESCE(
        s.token_value,
        b.token_value
    ) AS token_value,
    w._log_id,
    w._inserted_timestamp
FROM
    name_wrapped w
    LEFT JOIN transfer_single s
    ON w.block_number = s.block_number
    AND w.tx_hash = s.tx_hash
    LEFT JOIN transfer_batch b
    ON w.block_number = b.block_number
    AND w.tx_hash = b.tx_hash
