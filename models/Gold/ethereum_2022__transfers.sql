{{ config(
    materialized = 'view',
    tags = ['snowflake', 'ethereum', 'gold_ethereum', 'ethereum_transfers']
) }}

SELECT
    block_id,
    tx_hash,
    block_timestamp,
    contract_address :: STRING AS contract_address,
    event_inputs :from :: STRING AS from_address,
    event_inputs :to :: STRING AS to_address,
    event_inputs :value :: FLOAT AS raw_amount
FROM
    {{ ref('silver_ethereum_2022__transfers') }}
