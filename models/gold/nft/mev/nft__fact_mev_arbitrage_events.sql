{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }} }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    trade_side,
    event_index,
    platform_name,
    platform_exchange_version,
    buyer_address,
    seller_address,
    nft_address,
    tokenid,
    erc1155_value,
    project_name,
    funding_source,
    arbitrage_direction,
    COALESCE (
        nft_arbitrage_events_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index', 'trade_side', 'nft_address','tokenId','platform_exchange_version']
        ) }}
    ) AS ez_mev_arbitrage_events_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver_nft__arbitrage_events') }}
