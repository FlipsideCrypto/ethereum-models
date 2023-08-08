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
    VALUE AS eth_value,
    tx_fee,
    gas_price,
    gas AS gas_limit,
    gas_used,
    cumulative_Gas_Used,
    input_data,
    tx_status AS status,
    effective_gas_price,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    r,
    s,
    v,
    tx_type,
    chain_id
FROM
    {{ ref('silver__transactions') }}
