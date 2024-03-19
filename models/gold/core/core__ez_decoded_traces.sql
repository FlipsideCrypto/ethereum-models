{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    tx_hash,
    block_timestamp,
    tx_status,
    tx_position,
    trace_index,
    from_address,
    c0.name AS from_address_name,
    to_address,
    c1.name AS to_address_name,
    VALUE,
    value_precise_raw,
    value_precise,
    gas,
    gas_used,
    TYPE,
    identifier,
    sub_traces,
    error_reason,
    trace_status,
    input,
    output,
    decoded_data :function_name :: STRING AS function_name,
    decoded_data :decoded_input_data AS decoded_input_data,
    decoded_data :decoded_output_data AS decoded_output_data,
    decoded_traces_id AS fact_decoded_traces_id,
    GREATEST(COALESCE(t.inserted_timestamp, '2000-01-01'), COALESCE(c0.inserted_timestamp, '2000-01-01'), COALESCE(c1.inserted_timestamp, '2000-01-01')) AS inserted_timestamp,
    GREATEST(COALESCE(t.modified_timestamp, '2000-01-01'), COALESCE(c0.modified_timestamp, '2000-01-01'), COALESCE(c1.modified_timestamp, '2000-01-01')) AS modified_timestamp
FROM
    {{ ref('silver__decoded_traces') }}
    t
    LEFT JOIN {{ ref('silver__contracts') }}
    c0
    ON t.from_address = c0.address
    LEFT JOIN {{ ref('silver__contracts') }}
    c1
    ON t.to_address = c1.address
