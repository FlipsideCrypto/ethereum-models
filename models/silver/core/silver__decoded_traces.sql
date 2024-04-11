-- depends_on: {{ ref('bronze__decoded_traces') }}
{{ config (
    materialized = "incremental",
    unique_key = ['block_number', 'tx_position', 'trace_index'],
    cluster_by = "block_timestamp::date",
    incremental_predicates = ["dynamic_range", "block_number"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    merge_exclude_columns = ["inserted_timestamp"],
    full_refresh = false,
    tags = ['decoded_traces','reorg']
) }}

WITH base AS (

    SELECT
        block_number :: INTEGER AS block_number,
        id :: STRING AS _call_id,
        DATA AS decoded_data,
        _inserted_timestamp :: timestamp_ntz AS _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__decoded_traces') }}
WHERE
    _inserted_timestamp :: timestamp_ntz >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '2 hours'
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__fr_decoded_traces') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number, _call_id
ORDER BY
    _inserted_timestamp DESC, _partition_by_created_date DESC)) = 1
),
new_records AS (
    SELECT
        block_number,
        tx_hash,
        block_timestamp,
        tx_status,
        tx_position,
        trace_index,
        from_address,
        to_address,
        eth_value_precise AS value_precise,
        eth_value AS VALUE,
        eth_value_precise_raw AS value_precise_raw,
        gas,
        gas_used,
        TYPE,
        identifier,
        sub_traces,
        error_reason,
        trace_status,
        _call_id,
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
        LEFT JOIN {{ ref('silver__traces') }} USING (
            block_number,
            _call_id
        )
)

{% if is_incremental() %},
missing_data AS (
    SELECT
        t.block_number,
        tr.tx_hash,
        tr.block_timestamp,
        tr.tx_status,
        tr.trace_index,
        tr.tx_position,
        tr.from_address,
        tr.to_address,
        tr.eth_value_precise AS value_precise,
        tr.eth_value AS VALUE,
        tr.eth_value_precise_raw AS value_precise_raw,
        tr.gas,
        tr.gas_used,
        tr.type,
        tr.identifier,
        tr.sub_traces,
        tr.error_reason,
        tr.trace_status,
        t._call_id,
        t.decoded_data,
        tr.input,
        tr.output,
        t._inserted_timestamp,
        FALSE AS is_pending
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('silver__traces') }}
        tr USING (
            block_number,
            _call_id
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
    identifier,
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
    identifier,
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
