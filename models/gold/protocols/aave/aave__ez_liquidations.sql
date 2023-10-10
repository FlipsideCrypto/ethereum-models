{{ config(
    materialized = 'view',
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'AAVE',
                'PURPOSE': 'DEFI'
            }
        }
    },
    tags = ['non_realtime','reorg'],
    persist_docs ={ "relation": true,
    "columns": true }
) }}
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    collateral_asset,
    collateral_aave_token,
    liquidated_amount,
    liquidated_amount_usd,
    debt_asset,
    debt_aave_token,
    debt_to_cover_amount,
    debt_to_cover_amount_usd,
    liquidator,
    borrower,
    aave_version,
    collateral_token_price,
    collateral_token_symbol,
    debt_token_price,
    debt_token_symbol,
    blockchain,
    _log_id,
    _inserted_timestamp
FROM
    {{ref('silver__aave_ez_liquidations')}}