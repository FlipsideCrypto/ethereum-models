{{ config(
    materialized = 'view',
    tags = ['snowflake', 'ethereum', 'gold_ethereum', 'ethereum_transactions']
) }}

SELECT
    block_timestamp,
    block_number,
    block_hash,
    tx_hash,
    nonce,
    INDEX,
    from_address,
    to_address,
    eth_value,
    tx_fee,
    gas_price,
    gas_limit,
    gas_used,
    cumulativeGasUsed,
    status
FROM
    {{ ref('silver_ethereum_2022__transactions') }}
