 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'COMPOUND, SPARK, AAVE, FRAXLEND',
                'PURPOSE': 'LENDING, LIQUIDATIONS'
            }
        }
    }
) }}

SELECT
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_hash,
    event_index,
    contract_address,
    event_name,
    protocol_collateral_asset as protocol_market,
    liquidation_amount as amount,
    liquidation_amount_usd as amount_usd,
    collateral_asset,
    collateral_asset_symbol,
    debt_asset,
    debt_asset_symbol,
    liquidator,
    borrower,
    platform
FROM 
    {{ ref('silver__complete_lending_liquidations') }}