-- depends_on: {{ ref('bronze__streamline_decoded_traces') }}
{{ config (
    materialized = "incremental",
    unique_key = ['block_number', 'tx_position', 'trace_index'],
    cluster_by = "block_timestamp::date",
    incremental_predicates = ["dynamic_range", "block_number"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    merge_exclude_columns = ["inserted_timestamp"],
    full_refresh = false,
    tags = ['silver','decoded_traces']
) }}

WITH base AS (

    SELECT
        block_number :: INTEGER AS block_number,
        id :: STRING AS _call_id,
        DATA AS decoded_data,
        _inserted_timestamp :: timestamp_ntz AS _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_decoded_traces') }}
WHERE
    _inserted_timestamp :: timestamp_ntz >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '2 hours'
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_fr_decoded_traces') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number, _call_id
ORDER BY
    _inserted_timestamp DESC, _partition_by_created_date DESC)) = 1
),
new_records AS (
    SELECT
        b.block_number,
        tx_hash,
        block_timestamp,
        CASE
            WHEN tx_succeeded = TRUE THEN 'SUCCESS'
            ELSE 'FAIL'
        END AS tx_status,
        tx_position,
        trace_index,
        from_address,
        to_address,
        value_precise,
        VALUE,
        value_precise_raw,
        gas,
        gas_used,
        TYPE,
        sub_traces,
        error_reason,
        CASE 
            WHEN trace_succeeded = TRUE THEN 'SUCCESS'
            ELSE 'FAIL'
        END AS trace_status,
        concat_ws(
            '-',
            t.block_number,
            t.tx_position,
            CONCAT(
                t.type,
                '_',
                t.trace_address
            )
        ) AS _call_id,
        decoded_data,
        input,
        output,
        b._inserted_timestamp,
        CASE
            WHEN block_timestamp IS NULL THEN TRUE
            ELSE FALSE
        END AS is_pending
    FROM
        base b
        LEFT JOIN {{ ref('core__fact_traces') }}
        t
        ON b.block_number = t.block_number
        AND b._call_id = concat_ws(
            '-',
            t.block_number,
            t.tx_position,
            CONCAT(
                t.type,
                '_',
                t.trace_address
            )
        )
)

{% if is_incremental() %},
missing_data AS (
    SELECT
        t.block_number,
        tr.tx_hash,
        tr.block_timestamp,
        CASE
            WHEN tr.tx_succeeded = TRUE THEN 'SUCCESS'
            ELSE 'FAIL'
        END AS tx_status,
        tr.trace_index,
        tr.tx_position,
        tr.from_address,
        tr.to_address,
        tr.value_precise,
        tr.value,
        tr.value_precise_raw,
        tr.gas,
        tr.gas_used,
        tr.type,
        tr.sub_traces,
        tr.error_reason,
        CASE
            WHEN tr.trace_succeeded = TRUE THEN 'SUCCESS'
            ELSE 'FAIL'
        END AS trace_status,
        t._call_id,
        t.decoded_data,
        tr.input,
        tr.output,
        t._inserted_timestamp,
        FALSE AS is_pending
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('core__fact_traces') }}
        tr
        ON t.block_number = tr.block_number
        AND t._call_id = concat_ws(
            '-',
            tr.block_number,
            tr.tx_position,
            CONCAT(
                tr.type,
                '_',
                tr.trace_address
            )
        )
    WHERE
        t.is_pending
        AND tr.block_timestamp IS NOT NULL
)
{% endif %}
SELECT
    block_number,
    tx_hash,
    block_timestamp,
    tx_status,
    tx_position,
    trace_index,
    from_address,
    to_address,
    value_precise,
    VALUE,
    value_precise_raw,
    gas,
    gas_used,
    TYPE,
    sub_traces,
    error_reason,
    trace_status,
    _call_id,
    decoded_data,
    input,
    output,
    _inserted_timestamp,
    is_pending,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'trace_index']
    ) }} AS decoded_traces_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    new_records

{% if is_incremental() %}
UNION
SELECT
    block_number,
    tx_hash,
    block_timestamp,
    tx_status,
    tx_position,
    trace_index,
    from_address,
    to_address,
    value_precise,
    VALUE,
    value_precise_raw,
    gas,
    gas_used,
    TYPE,
    sub_traces,
    error_reason,
    trace_status,
    _call_id,
    decoded_data,
    input,
    output,
    _inserted_timestamp,
    is_pending,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'trace_index']
    ) }} AS decoded_traces_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    missing_data
{% endif %}
