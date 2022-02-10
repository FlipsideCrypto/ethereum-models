{{ config(
    materialized = 'view',
    tags = ['snowflake', 'ethereum', 'gold_ethereum', 'ethereum_transactions']
) }}

SELECT
    block_timestamp,
    block_number,
    tx_hash,
    nonce,
    INDEX,
    from_address,
    to_address,
    VALUE,
    block_hash,
    gas_price,
    gas,
    DATA,
    status
FROM
    {{ ref('silver_ethereum_2022__transactions') }}
