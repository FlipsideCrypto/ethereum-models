{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'COMPOUND',
                'PURPOSE': 'DEFI'
            }
        }
    },
    tags = ['non_realtime']
) }}
SELECT
    ctoken_address,
    ctoken_symbol,
    ctoken_name,
    ctoken_decimals,
    underlying_asset_address,
    underlying_name,
    underlying_symbol,
    underlying_decimals,
    underlying_contract_metadata,
    created_block
FROM
    {{ ref('silver__compv2_asset_details') }}