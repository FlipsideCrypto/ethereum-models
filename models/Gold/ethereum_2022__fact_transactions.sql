{{ config(
    materialized = 'view'
) }}

SELECT
    block_number,
    block_timestamp,
    block_hash,
    tx_hash,
    nonce,
    POSITION,
    from_address,
    to_address,
    eth_value,
    tx_fee,
    gas_price,
    gas_limit,
    gas_used,
    cumulative_Gas_Used,
    status
FROM
    {{ ref('silver_ethereum_2022__transactions') }}
