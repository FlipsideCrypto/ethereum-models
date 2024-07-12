{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' } } }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    mev_searcher,
    mev_contract,
    total_cost_usd,
    total_revenue_usd,
    net_profit_usd,
    tx_fee,
    tx_fee_usd,
    input_data -- add PK
FROM
    {{ ref('silver_nft__arbitrage_trades') }}
