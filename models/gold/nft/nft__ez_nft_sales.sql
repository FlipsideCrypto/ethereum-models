{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }
    } }
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
    nft_address AS contract_address, --new column
    project_name AS NAME, --new column
    tokenid AS token_id, --new column
    COALESCE(
        erc1155_value,
        '1'
    ) :: STRING AS quantity, --new column
    CASE
        WHEN erc1155_value IS NULL THEN 'erc721'
        ELSE 'erc1155'
    END AS token_standard, --new column
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
    ) AS modified_timestamp,
    tokenId, --deprecate
    erc1155_value, --deprecate
    project_name, --deprecate
    nft_address --deprecate
FROM
    {{ ref('silver_nft__complete_nft_sales') }}
