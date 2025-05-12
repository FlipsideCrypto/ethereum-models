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
    trade_side,
    event_index,
    platform_name,
    platform_exchange_version,
    buyer_address,
    seller_address,
    nft_address AS contract_address, 
    tokenid AS token_id, 
    COALESCE(
        erc1155_value,
        '1'
    ) :: STRING AS quantity, 
    CASE
        WHEN erc1155_value IS NULL THEN 'erc721'
        ELSE 'erc1155'
    END AS token_standard, 
    project_name AS name, 
    nft_arbitrage_events_id AS ez_mev_arbitrage_events_id,
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
