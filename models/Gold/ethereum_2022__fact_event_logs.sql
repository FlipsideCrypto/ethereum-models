{{ config(
    materialized = 'view'
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    event_name,
    event_inputs,
    topics,
    DATA,
    event_removed,
    _log_id
FROM
    {{ ref('silver_ethereum_2022__logs') }}
