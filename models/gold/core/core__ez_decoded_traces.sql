{{ config (
    materialized = "incremental",
    unique_key = "ez_decoded_traces_id",
    incremental_strategy = 'delete+insert',
    cluster_by = "block_timestamp::date",
    incremental_predicates = ["dynamic_range", "block_number"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(ez_decoded_traces_id, from_address_name, to_address_name, from_address, to_address)",
    tags = ['decoded_traces']
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
    decoded_data AS full_decoded_trace, --new column
    decoded_data :function_name :: STRING AS function_name,
    decoded_data :decoded_input_data AS decoded_input_data,
    decoded_data :decoded_output_data AS decoded_output_data,
    TYPE,
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
    decoded_traces_id AS ez_decoded_traces_id,
{% if is_incremental() %}
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
{% else %}
    GREATEST(block_timestamp, DATEADD('day', -10, SYSDATE())) AS inserted_timestamp,
    GREATEST(block_timestamp, DATEADD('day', -10, SYSDATE())) AS modified_timestamp,
{% endif %}
    identifier, --deprecate
    trace_status, --deprecate
    tx_status, --deprecate
    decoded_traces_id AS fact_decoded_traces_id --deprecate
FROM
    {{ ref('silver__decoded_traces') }}
    t
    LEFT JOIN {{ ref('silver__contracts') }}
    c0
    ON t.from_address = c0.address
    LEFT JOIN {{ ref('silver__contracts') }}
    c1
    ON t.to_address = c1.address
WHERE 1=1

    {% if is_incremental() %}
    AND t.modified_timestamp > (
        SELECT
            COALESCE(
                MAX(modified_timestamp),
                '2000-01-01'::TIMESTAMP
            )
        FROM
            {{ this }}
    )
    {% endif %}