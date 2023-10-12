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
    "columns": true },
    tags = ['non_realtime']
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
    redeemer
FROM
    {{ ref('silver__compv2_redemptions') }}
