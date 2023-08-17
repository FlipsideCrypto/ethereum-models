{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
    'database_tags':{
        'table': {
            'PROTOCOL': 'BALANCER, CURVE, DODO, FRAXSWAP, KYBERSWAP, MAVERICK, PANCAKESWAP, SHIBASWAP, SUSHISWAP, UNISWAP, TRADER JOE',
            'PURPOSE': 'DEX, LIQUIDITY, POOLS, LP, SWAPS',
            }
        }
    }
) }}

SELECT
    block_number AS creation_block,
    block_timestamp AS creation_time,
    tx_hash AS creation_tx,
    platform,
    contract_address AS factory_address,
    pool_address,
    pool_name,
    tokens,
    symbols,
    decimals
FROM
    {{ ref('silver_dex__complete_dex_liquidity_pools') }}