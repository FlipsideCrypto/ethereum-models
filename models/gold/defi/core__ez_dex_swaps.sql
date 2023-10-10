{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'SUSHI, UNISWAP, CURVE, SYNTHETIX, BALANCER, DODO, FRAX, HASHFLOW, KYBERSWAP, MAVERICK, PANCAKESWAP, SHIBASWAP, TRADER JOE',
    'PURPOSE': 'DEX, SWAPS' } } }
) }}

SELECT
    *
FROM
    {{ ref('defi__ez_dex_swaps') }}
