{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    A.block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    event_name,
    decoded_flat AS decoded_log,
    decoded_data AS full_decoded_log
FROM
    {{ ref('silver__decoded_logs') }} A
    LEFT JOIN {{ ref('silver__blocks') }} USING (block_number)
