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
    buy_event_index,
    buy_platform_name,
    buy_platform_exchange_version,
    buy_buyer_address,
    buy_seller_address,
    buy_nft_address,
    buy_tokenid,
    buy_erc1155_value,
    buy_project_name -- add PK
FROM
    {{ ref('silver_nft__arbitrage_buy_side') }}
