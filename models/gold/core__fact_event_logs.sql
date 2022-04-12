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
    contract_name,
    event_name,
    event_inputs,
    topics,
    DATA,
    event_removed,
    _log_id
FROM
    {{ ref('silver__logs') }}
