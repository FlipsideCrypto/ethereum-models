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
    eth_value AS VALUE,
    eth_value_precise_raw AS value_precise_raw,
    eth_value_precise AS value_precise,
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
    COALESCE (
        traces_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'trace_index']
        ) }}
    ) AS fact_traces_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp,
    eth_value,
    eth_value_precise_raw,
    eth_value_precise
FROM
    {{ ref('silver__traces') }}
