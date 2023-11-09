{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    tx_hash,
    block_number,
    block_timestamp,
    identifier,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    from_address AS eth_from_address,
    to_address AS eth_to_address,
    amount,
    amount_precise_raw,
    amount_precise,
    amount_usd,
    _call_id,
    _inserted_timestamp,
    tx_position,
    trace_index
FROM
    {{ ref('silver__native_transfers') }}
