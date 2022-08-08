{{ config(
    materialized = 'incremental',
    unique_key = "tx_hash",
    cluster_by = ['block_timestamp::date'],
    tags = ['share']
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
    where block_timestamp::date between '2021-12-01' and '2021-12-31'