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
    from_address,
    to_address,
    'ETH' AS symbol,
    amount,
    amount_precise_raw,
    amount_precise,
    amount_usd,
    inserted_timestamp,
    modified_timestamp,
    native_transfers_id AS ez_native_transfers_id
FROM
    {{ ref('silver__native_transfers') }}
