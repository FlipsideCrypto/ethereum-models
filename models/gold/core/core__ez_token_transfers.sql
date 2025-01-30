{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    from_address,
    to_address,
    raw_amount_precise,
    raw_amount,
    amount_precise,
    amount,
    amount_usd,
    decimals,
    symbol,
    token_price,
    has_decimal,
    has_price,
    COALESCE (
        transfers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS ez_token_transfers_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp,
    _log_id, --deprecate
    _inserted_timestamp --deprecate
FROM
    {{ ref('silver__transfers') }}
