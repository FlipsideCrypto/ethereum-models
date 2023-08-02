{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['core']
) }}

SELECT
    tx_hash,
    block_number,
    block_timestamp,
    from_address,
    to_address,
    eth_value,
    utils.udf_hex_to_int(
        DATA :value :: STRING
    ) AS eth_value_precise_raw,
    utils.udf_decimal_adjust(
        eth_value_precise_raw,
        18
    ) AS eth_value_precise,,
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
    trace_index
FROM
    {{ ref('silver__traces') }}
