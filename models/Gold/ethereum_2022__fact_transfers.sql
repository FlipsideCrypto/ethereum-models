{{ config(
    materialized = 'view',
    tags = ['snowflake', 'ethereum', 'gold_ethereum', 'ethereum_transfers']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    from_address,
    to_address,
    raw_amount
FROM
    {{ ref('silver_ethereum_2022__transfers') }}
