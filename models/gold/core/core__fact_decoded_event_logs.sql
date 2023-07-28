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
    event_name,
    decoded_flat AS decoded_log,
    decoded_data AS full_decoded_log
FROM
    {{ ref('silver__decoded_logs') }}
