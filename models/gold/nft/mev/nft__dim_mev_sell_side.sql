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
    sell_event_index,
    sell_platform_name,
    sell_platform_exchange_version,
    sell_buyer_address,
    sell_seller_address,
    sell_nft_address,
    sell_tokenid,
    sell_erc1155_value,
    sell_project_name -- add PK
FROM
    {{ ref('silver_nft__arbitrage_sell_side') }}
