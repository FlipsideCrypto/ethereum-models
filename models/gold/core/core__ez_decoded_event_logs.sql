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
    topics,
    topics[0]::STRING AS topic_0, --new column
    topics[1]::STRING AS topic_1, --new column
    topics[2]::STRING AS topic_2, --new column
    topics[3]::STRING AS topic_3, --new column
    DATA,
    event_removed,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    CASE
        WHEN tx_status = 'SUCCESS' THEN TRUE
        ELSE FALSE
    END AS tx_succeeded, --new column
    event_name,
    decoded_data AS full_decoded_log,
    decoded_flat AS decoded_log,
    C.name AS contract_name,
    COALESCE (
        decoded_logs_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS ez_decoded_event_logs_id,
    GREATEST(COALESCE(l.inserted_timestamp, '2000-01-01'), COALESCE(C.inserted_timestamp, '2000-01-01')) AS inserted_timestamp,
    GREATEST(COALESCE(l.modified_timestamp, '2000-01-01'), COALESCE(C.modified_timestamp, '2000-01-01')) AS modified_timestamp,
    tx_status --deprecate
FROM
    {{ ref('silver__decoded_logs') }}
    l
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON l.contract_address = C.address
