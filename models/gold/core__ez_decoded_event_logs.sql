{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    C.name AS contract_name,
    event_name,
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
    {{ ref('silver__decoded_logs') }}
    LEFT JOIN {{ ref('core__dim_contracts') }} C
    ON contract_address = C.address
