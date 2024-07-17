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
    cost_usd,
    revenue_usd,
    profit_usd,
    tx_fee,
    tx_fee_usd,
    funding_source,
    arbitrage_direction,
    COALESCE (
        nft_arbitrage_trades_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'nft_log_id']
        ) }}
    ) AS ez_mev_arbitrage_id,
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
