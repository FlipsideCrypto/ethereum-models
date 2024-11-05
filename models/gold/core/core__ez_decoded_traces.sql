{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    tx_position,
    trace_index,    
    from_address,
    c0.name AS from_address_name,
    to_address,
    c1.name AS to_address_name,
    input,
    output,
    decoded_data :function_name :: STRING AS function_name,
    decoded_data AS full_decoded_data, --new column
    decoded_data :decoded_input_data AS decoded_input_data,
    decoded_data :decoded_output_data AS decoded_output_data,
    TYPE,
    {# trace_address, --new column, requires FR on silver.decoded_traces #}
    sub_traces,
    VALUE,
    value_precise_raw,
    value_precise,
    gas,
    gas_used,
    CASE
        WHEN trace_status = 'SUCCESS' THEN TRUE
        ELSE FALSE
    END AS trace_succeeded, --new column
    error_reason,
    CASE
        WHEN tx_status = 'SUCCESS' THEN TRUE
        ELSE FALSE
    END AS tx_succeeded, --new column
    decoded_traces_id AS fact_decoded_traces_id,
    GREATEST(COALESCE(t.inserted_timestamp, '2000-01-01'), COALESCE(c0.inserted_timestamp, '2000-01-01'), COALESCE(c1.inserted_timestamp, '2000-01-01')) AS inserted_timestamp,
    GREATEST(COALESCE(t.modified_timestamp, '2000-01-01'), COALESCE(c0.modified_timestamp, '2000-01-01'), COALESCE(c1.modified_timestamp, '2000-01-01')) AS modified_timestamp,
    identifier, --deprecate
    trace_status, --deprecate
    tx_status --deprecate
FROM
    {{ ref('silver__decoded_traces') }}
    t
    LEFT JOIN {{ ref('silver__contracts') }}
    c0
    ON t.from_address = c0.address
    LEFT JOIN {{ ref('silver__contracts') }}
    c1
    ON t.to_address = c1.address
