{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    tx_position, --new column
    trace_index,
    from_address,
    to_address,
    input,
    output,
    TYPE,
    trace_address, --new column
    sub_traces,
    VALUE,
    value_precise_raw,
    value_precise,
    value_hex, --new column
    gas,
    gas_used,
    origin_from_address, --new column
    origin_to_address, --new column
    origin_function_signature, --new column
    trace_succeeded, --new column
    error_reason,
    revert_reason, --new column
    tx_succeeded, --new column
    fact_traces_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp,
    identifier, --deprecate
    tx_status, --deprecate
    DATA, --deprecate
    trace_status --deprecate    
FROM
    {{ ref('silver__fact_traces2') }}
--ideal state = source from silver.traces2 and materialize this model as a table (core.fact_traces2)