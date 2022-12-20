{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    decoded_data AS full_decoded_log,
    decoded_flat as decoded_log
FROM
    {{ ref('silver__decoded_logs') }}
