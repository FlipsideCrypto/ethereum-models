{{ config(
    materialized = 'view',
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'COMPOUND',
                'PURPOSE': 'DEFI'
            }
        }
    },
    persist_docs ={ "relation": true,
    "columns": true }
) }}
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    ctoken,
    ctoken_symbol,
    received_amount,
    received_amount_usd,
    received_contract_address,
    received_contract_symbol,
    redeemed_ctoken,
    redeemer,
    compound_version as version,
    _inserted_timestamp,
    _log_id,
    COALESCE (
        comp_redemptions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS ez_redemptions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__comp_redemptions') }}
