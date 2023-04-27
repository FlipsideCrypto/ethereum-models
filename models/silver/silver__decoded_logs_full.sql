{{ config (
    materialized = "incremental",
    unique_key = "_log_id",
    cluster_by = "block_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH decoded_logs AS (

    SELECT
        tx_hash,
        block_number,
        event_index,
        event_name,
        contract_address,
        decoded_data,
        transformed,
        _log_id,
        _inserted_timestamp,
        decoded_flat
    FROM
        {{ ref('silver__decoded_logs') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
),
new_records AS (
    SELECT
        b.tx_hash,
        b.block_number,
        b.event_index,
        b.event_name,
        b.contract_address,
        b.decoded_data,
        b.transformed,
        b._log_id,
        b._inserted_timestamp,
        b.decoded_flat,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        topics,
        DATA,
        event_removed,
        tx_status,
        CASE
            WHEN block_timestamp IS NULL THEN TRUE
            ELSE FALSE
        END AS is_pending
    FROM
        decoded_logs b
        LEFT JOIN {{ ref('silver__logs') }} USING (
            block_number,
            _log_id
        )
)

{% if is_incremental() %},
missing_data AS (
    SELECT
        t.tx_hash,
        t.block_number,
        t.event_index,
        t.event_name,
        t.contract_address,
        t.decoded_data,
        t.transformed,
        t._log_id,
        GREATEST(
            t._inserted_timestamp,
            l._inserted_timestamp
        ) AS _inserted_timestamp,
        t.decoded_flat,
        l.block_timestamp,
        l.origin_function_signature,
        l.origin_from_address,
        l.origin_to_address,
        l.topics,
        l.data,
        l.event_removed,
        l.tx_status,
        FALSE AS is_pending
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('silver__logs') }}
        l USING (
            block_number,
            _log_id
        )
    WHERE
        t.is_pending
)
{% endif %}
SELECT
    tx_hash,
    block_number,
    event_index,
    event_name,
    contract_address,
    decoded_data,
    transformed,
    _log_id,
    _inserted_timestamp,
    decoded_flat,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    topics,
    DATA,
    event_removed,
    tx_status,
    is_pending
FROM
    new_records

{% if is_incremental() %}
UNION
SELECT
    tx_hash,
    block_number,
    event_index,
    event_name,
    contract_address,
    decoded_data,
    transformed,
    _log_id,
    _inserted_timestamp,
    decoded_flat,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    topics,
    DATA,
    event_removed,
    tx_status,
    is_pending
FROM
    missing_data
{% endif %}
