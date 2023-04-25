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
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    aggregator_name,
    seller_address,
    buyer_address,
    nft_address,
    project_name,
    erc1155_value,
    tokenId,
    token_metadata,
    currency_symbol,
    currency_address,
    price,
    price_usd,
    total_fees,
    platform_fee,
    creator_fee,
    total_fees_usd,
    platform_fee_usd,
    creator_fee_usd,
    tx_fee,
    tx_fee_usd,
    origin_from_address,
    origin_to_address,
    origin_function_signature
FROM
    {{ ref('silver_nft__complete_nft_sales') }}
    b
    LEFT JOIN {{ ref('silver__nft_labels_temp') }}
    l
    ON b.nft_address = l.project_address
    AND b.tokenId = l.token_id
