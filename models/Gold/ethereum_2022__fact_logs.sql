{{ config(
    materialized = 'view',
    tags = ['snowflake', 'ethereum', 'gold_ethereum', 'ethereum_logs']
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
    event_removed
FROM
    {{ ref('silver_ethereum_2022__logs') }}
