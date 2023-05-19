{{ config(
    materialized = 'incremental',
    unique_key = 'nft_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH nft_base_models AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_type,
        platform_address,
        platform_name,
        platform_exchange_version,
        seller_address,
        buyer_address,
        nft_address,
        erc1155_value,
        tokenId,
        currency_address,
        total_price_raw,
        total_fees_raw,
        platform_fee_raw,
        creator_fee_raw,
        tx_fee,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        input_data,
        nft_log_id,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_nft__wyvern_decoded_sales') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address,
    erc1155_value,
    tokenId,
    currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    tx_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    input_data,
    nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver_nft__looksrare_decoded_sales') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address,
    erc1155_value,
    tokenId,
    currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    tx_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    input_data,
    nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver_nft__rarible_decoded_sales') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address,
    erc1155_value,
    tokenId,
    currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    tx_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    input_data,
    nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver_nft__x2y2_decoded_sales') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address,
    erc1155_value,
    tokenId,
    currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    tx_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    input_data,
    nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver_nft__nftx_decoded_sales') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address,
    erc1155_value,
    tokenId,
    currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    tx_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    input_data,
    nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver_nft__seaport_1_1_sales') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address,
    erc1155_value,
    tokenId,
    currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    tx_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    input_data,
    nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver_nft__cryptopunk_sales') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address,
    erc1155_value,
    tokenId,
    currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    tx_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    input_data,
    nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver_nft__sudoswap_sales') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address,
    erc1155_value,
    tokenId,
    currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    tx_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    input_data,
    nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver_nft__blur_decoded_sales') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address,
    erc1155_value,
    tokenId,
    currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    tx_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    input_data,
    nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver_nft__seaport_1_4_sales') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address,
    erc1155_value,
    tokenId,
    currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    tx_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    input_data,
    nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver_nft__looksrare_v2') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address,
    erc1155_value,
    tokenId,
    currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    tx_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    input_data,
    nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver_nft__seaport_1_5_sales') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
),
labels_only AS (
    SELECT
        DISTINCT project_address AS project_address,
        project_name
    FROM
        {{ ref('silver__nft_labels_temp') }}
    WHERE
        project_address IS NOT NULL
),
metadata AS (
    SELECT
        project_address,
        token_id,
        token_metadata
    FROM
        {{ ref('silver__nft_labels_temp') }}
    WHERE
        project_address IS NOT NULL
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    CASE
        WHEN origin_to_address IN (
            '0xf24629fbb477e10f2cf331c2b7452d8596b5c7a5',
            '0x83c8f28c26bf6aaca652df1dbbe0e1b56f8baba2'
        ) THEN 'gem'
        WHEN origin_to_address IN (
            '0x39da41747a83aee658334415666f3ef92dd0d541',
            '0x000000000000ad05ccc4f10045630fb830b95127'
        ) THEN 'blur'
        ELSE platform_name
    END AS platform_name,
    platform_exchange_version,
    CASE
        WHEN RIGHT(
            input_data,
            8
        ) = '72db8c0b'
        AND block_timestamp :: DATE <= '2023-04-04' THEN 'Gem'
        WHEN RIGHT(
            input_data,
            8
        ) = '72db8c0b'
        AND block_timestamp :: DATE >= '2023-04-05' THEN 'OpenSea Pro'
        WHEN RIGHT(
            input_data,
            8
        ) = '332d1229' THEN 'Blur'
        ELSE NULL
    END AS aggregator_name,
    seller_address,
    buyer_address,
    nft_address,
    l.project_name,
    erc1155_value,
    tokenId,
    m.token_metadata,
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
    nft_log_id,
    input_data,
    _log_id,
    _inserted_timestamp
FROM
    nft_base_models b
    LEFT JOIN labels_only l
    ON b.nft_address = l.project_address
    LEFT JOIN metadata m
    ON b.nft_address = m.project_address
    AND b.tokenId = m.token_id all_prices AS (
        SELECT
            HOUR,
            symbol,
            token_address AS currency_address,
            decimals,
            price AS hourly_prices
        FROM
            {{ ref('core__fact_hourly_token_prices') }}
        WHERE
            (
                currency_address IN (
                    SELECT
                        DISTINCT currency_address
                    FROM
                        --final_base
                )
            )
            AND HOUR :: DATE IN (
                SELECT
                    DISTINCT block_timestamp :: DATE
                FROM
                    tx_data
            )
    ),
    manual_eth_prices AS (
        SELECT
            HOUR,
            'ETH' AS symbol,
            'ETH' AS currency_address,
            decimals,
            price AS hourly_prices
        FROM
            all_prices
        WHERE
            token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            AND HOUR :: DATE IN (
                SELECT
                    DISTINCT block_timestamp :: DATE
                FROM
                    tx_data
            )
            AND HOUR :: DATE >= '2023-03-01'
        UNION ALL
        SELECT
            HOUR,
            'ETH' AS symbol,
            '0x0000000000a39bb272e79075ade125fd351887ac' AS currency_address,
            -- blurETH
            decimals,
            price AS hourly_prices
        FROM
            all_prices
        WHERE
            token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    ),
    all_prices_combined
