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
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    collateral_asset,
    liquidator,
    borrower,
    liquidation_amount,
    liquidation_amount_usd,
    protocol_collateral_asset,
    protocol_collateral_symbol,
    protocol_debt_asset,
    debt_asset,
    debt_asset_symbol,
    debt_to_cover_amount,
    debt_to_cover_amount_usd,
    platform,
    blockchain
FROM 
    {{ ref('silver__complete_lending_liquidations') }}