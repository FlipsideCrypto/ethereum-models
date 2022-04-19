{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    block_hash,
    tx_hash,
    nonce,
    POSITION,
    origin_function_signature,
    from_address,
    to_address,
    eth_value,
    tx_fee,
    gas_price,
    gas_limit,
    gas_used,
    cumulative_Gas_Used,
    input_data,
    status,
    tx_json
FROM
    {{ ref('silver__transactions') }}
