{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' } } },
    tags = ['gold','nft','curated','ez']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    mev_searcher,
    mev_contract,
    cost_usd,
    revenue_usd,
    miner_tip_usd, 
    tx_fee_usd,
    profit_usd,
    funding_source,
    arbitrage_direction,
    nft_arbitrage_trades_id AS ez_mev_arbitrage_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver_nft__arbitrage_trades') }}
