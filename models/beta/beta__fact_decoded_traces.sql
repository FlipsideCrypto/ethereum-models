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
    to_address,
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
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__decoded_traces') }}
