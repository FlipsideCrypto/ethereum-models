{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    tx_hash,
    block_number,
    block_timestamp,
    tx_position,
    trace_index,
    identifier,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    eth_from_address AS from_address,
    eth_to_address AS to_address,
    'ETH' AS symbol,
    amount,
    amount_precise_raw,
    amount_precise,
    amount_usd,
    _call_id,
    _inserted_timestamp
FROM
    {{ ref('core__ez_eth_transfers') }}
