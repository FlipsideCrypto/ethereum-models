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
    event_index,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    aggregator_name,
    seller_address,
    buyer_address,
    nft_address as contract_address,
    project_name as name,
    tokenId as token_id,
    coalesce(erc1155_value, '1')::STRING AS quantity,
    case when erc1155_value is null then 'erc721' else 'erc1155' end as token_standard,
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
    origin_function_signature,
    COALESCE (
        complete_nft_sales_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index', 'nft_address','tokenId','platform_exchange_version']
        ) }}
    ) AS ez_nft_sales_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp

FROM
    {{ ref('silver__complete_nft_sales') }}
