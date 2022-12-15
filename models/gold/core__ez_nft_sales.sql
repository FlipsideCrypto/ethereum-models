{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    CASE
        WHEN RIGHT(
            input_data,
            8
        ) = '72db8c0b' THEN 'Gem'
        WHEN RIGHT(
            input_data,
            8
        ) = '332d1229'
        OR origin_to_address IN (
            '0x39da41747a83aee658334415666f3ef92dd0d541',
            '0x000000000000ad05ccc4f10045630fb830b95127'
        )
        THEN 'Blur'
        ELSE NULL
    END AS aggregator_name,
    nft_from_address AS seller_address,
    nft_to_address AS buyer_address,
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
    {{ ref('silver_nft__opensea_sales') }}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    CASE
        WHEN RIGHT(
            input_data,
            8
        ) = '72db8c0b' THEN 'Gem'
        WHEN RIGHT(
            input_data,
            8
        ) = '332d1229'
        OR origin_to_address IN (
            '0x39da41747a83aee658334415666f3ef92dd0d541',
            '0x000000000000ad05ccc4f10045630fb830b95127'
        )
        THEN 'Blur'
        ELSE NULL
    END AS aggregator_name,
    nft_from_address AS seller_address,
    nft_to_address AS buyer_address,
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
    {{ ref('silver_nft__looksrare_sales') }}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    CASE
        WHEN RIGHT(
            input_data,
            8
        ) = '72db8c0b' THEN 'Gem'
        WHEN RIGHT(
            input_data,
            8
        ) = '332d1229'
        OR origin_to_address IN (
            '0x39da41747a83aee658334415666f3ef92dd0d541',
            '0x000000000000ad05ccc4f10045630fb830b95127'
        )
        THEN 'Blur'
        ELSE NULL
    END AS aggregator_name,
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
    {{ ref('silver_nft__rarible_sales') }}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    CASE
        WHEN RIGHT(
            input_data,
            8
        ) = '72db8c0b' THEN 'Gem'
        WHEN RIGHT(
            input_data,
            8
        ) = '332d1229'
        OR origin_to_address IN (
            '0x39da41747a83aee658334415666f3ef92dd0d541',
            '0x000000000000ad05ccc4f10045630fb830b95127'
        )
        THEN 'Blur'
        ELSE NULL
    END AS aggregator_name,
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
    {{ ref('silver_nft__x2y2_sales') }}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    CASE
        WHEN RIGHT(
            input_data,
            8
        ) = '72db8c0b' THEN 'Gem'
        WHEN RIGHT(
            input_data,
            8
        ) = '332d1229'
        OR origin_to_address IN (
            '0x39da41747a83aee658334415666f3ef92dd0d541',
            '0x000000000000ad05ccc4f10045630fb830b95127'
        )
        THEN 'Blur'
        ELSE NULL
    END AS aggregator_name,
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
    {{ ref('silver_nft__nftx_sales') }}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    CASE
        WHEN RIGHT(
            input_data,
            8
        ) = '72db8c0b' THEN 'Gem'
        WHEN RIGHT(
            input_data,
            8
        ) = '332d1229'
        OR origin_to_address IN (
            '0x39da41747a83aee658334415666f3ef92dd0d541',
            '0x000000000000ad05ccc4f10045630fb830b95127'
        )
        THEN 'Blur'
        ELSE NULL
    END AS aggregator_name,
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
    {{ ref('silver_nft__seaport_sales') }}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    CASE
        WHEN RIGHT(
            input_data,
            8
        ) = '72db8c0b' THEN 'Gem'
        WHEN RIGHT(
            input_data,
            8
        ) = '332d1229'
        OR origin_to_address IN (
            '0x39da41747a83aee658334415666f3ef92dd0d541',
            '0x000000000000ad05ccc4f10045630fb830b95127'
        )
        THEN 'Blur'
        ELSE NULL
    END AS aggregator_name,
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
    {{ ref('silver_nft__cryptopunk_sales') }}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    CASE
        WHEN RIGHT(
            input_data,
            8
        ) = '72db8c0b' THEN 'Gem'
        WHEN RIGHT(
            input_data,
            8
        ) = '332d1229'
        OR origin_to_address IN (
            '0x39da41747a83aee658334415666f3ef92dd0d541',
            '0x000000000000ad05ccc4f10045630fb830b95127'
        )
        THEN 'Blur'
        ELSE NULL
    END AS aggregator_name,
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
    {{ ref('silver_nft__sudoswap_sales') }}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    CASE
        WHEN RIGHT(
            input_data,
            8
        ) = '72db8c0b' THEN 'Gem'
        WHEN RIGHT(
            input_data,
            8
        ) = '332d1229'
        OR origin_to_address IN (
            '0x39da41747a83aee658334415666f3ef92dd0d541',
            '0x000000000000ad05ccc4f10045630fb830b95127'
        )
        THEN 'Blur'
        ELSE NULL
    END AS aggregator_name,
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
    {{ ref('silver_nft__rarible_sales_update_sept_2022') }}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    CASE
        WHEN RIGHT(
            input_data,
            8
        ) = '72db8c0b' THEN 'Gem'
        WHEN RIGHT(
            input_data,
            8
        ) = '332d1229'
        OR origin_to_address IN (
            '0x39da41747a83aee658334415666f3ef92dd0d541',
            '0x000000000000ad05ccc4f10045630fb830b95127'
        )
        THEN 'Blur'
        ELSE NULL
    END AS aggregator_name,
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
    {{ ref('silver_nft__blur_sales') }}
