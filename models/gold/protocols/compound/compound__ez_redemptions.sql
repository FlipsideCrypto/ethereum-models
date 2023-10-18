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
    compound_version
FROM
    {{ ref('silver__comp_redemptions') }}
