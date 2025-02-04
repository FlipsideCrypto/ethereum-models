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
    nft_address AS contract_address, --new column
    tokenid AS token_id, --new column
    COALESCE(
        erc1155_value,
        '1'
    ) :: STRING AS quantity, --new column
    CASE
        WHEN erc1155_value IS NULL THEN 'erc721'
        ELSE 'erc1155'
    END AS token_standard, --new column
    project_name AS name, --new column
    nft_arbitrage_events_id AS ez_mev_arbitrage_events_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp,
    nft_address, --deprecate
    tokenid, --deprecate
    erc1155_value, --deprecate
    project_name --deprecate
FROM
    {{ ref('silver_nft__arbitrage_events') }}
