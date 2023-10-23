{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    tx_hash,
    block_number,
    block_timestamp,
    from_address,
    to_address,
    eth_value AS native_value,
    eth_value_precise_raw AS native_value_precise_raw,
    eth_value_precise AS native_value_precise,
    gas,
    gas_used,
    input,
    output,
    TYPE,
    identifier,
    DATA,
    tx_status,
    sub_traces,
    trace_status,
    error_reason,
    trace_index,
    eth_value,
    eth_value_precise_raw,
    eth_value_precise
FROM
    {{ ref('silver__traces') }}
