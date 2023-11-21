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
    tx_status,
    COALESCE (
        decoded_logs_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS ez_decoded_event_logs_id,
    GREATEST(
        l.inserted_timestamp,
        C.inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    GREATEST(
        l.modified_timestamp,
        C.modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__decoded_logs') }}
    l
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON l.contract_address = C.address
