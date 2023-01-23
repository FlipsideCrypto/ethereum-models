{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    A.block_number,
    block_timestamp,
    A.tx_hash,
    A.event_index,
    A.contract_address,
    C.name AS contract_name,
    A.event_name,
    decoded_flat AS decoded_log,
    decoded_data AS full_decoded_log,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    topics,
    DATA,
    event_removed,
    tx_status
FROM
    {{ ref('silver__decoded_logs') }} A
    LEFT JOIN {{ ref('silver__logs') }} USING (
        _log_id,
        block_number
    )
    LEFT JOIN {{ ref('core__dim_contracts') }} C
    ON A.contract_address = C.address
