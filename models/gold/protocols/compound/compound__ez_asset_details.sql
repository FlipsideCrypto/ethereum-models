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
    'V2'AS compound_version, 
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
UNION ALL
SELECT
    'V3'AS compound_version,
    compound_market_address AS ctoken_address,
    compound_market_symbol AS ctoken_symbol,
    compound_market_name AS ctoken_name,
    compound_market_decimals AS ctoken_decimals,
    underlying_asset_address,
    underlying_asset_name AS underlying_name,
    underlying_asset_symbol AS underlying_symbol,
    underlying_asset_decimals AS underlying_decimals,
    NULL as underlying_contract_metadata,
    created_block
FROM
    {{ ref('silver__compv3_asset_details') }}