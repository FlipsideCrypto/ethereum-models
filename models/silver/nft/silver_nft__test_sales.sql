{{ config(
    materialized = 'incremental',
    unique_key = 'nft_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base_table AS (

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
        --currency_symbol,
        currency_address,
        total_price_raw,
        --price,
        -- price_usd,
        total_fees_raw,
        --total_fees,
        platform_fee_raw,
        --platform_fee,
        creator_fee_raw,
        -- creator_fee,
        --total_fees_usd,
        --platform_fee_usd,
        --creator_fee_usd,
        tx_fee,
        --tx_fee_usd,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        input_data,
        nft_log_id,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_nft__blur_decoded_sales') }}
),
all_prices AS (
    SELECT
        HOUR,
        symbol,
        token_address AS currency_address,
        decimals,
        price AS hourly_prices
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        HOUR :: DATE IN (
            SELECT
                block_timestamp :: DATE
            FROM
                base_table
        )
    UNION ALL
    SELECT
        HOUR,
        'ETH' AS symbol,
        '0x0000000000a39bb272e79075ade125fd351887ac' AS currency_address,
        decimals,
        price AS hourly_prices
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND HOUR :: DATE IN (
            SELECT
                block_timestamp :: DATE
            FROM
                base_table
        )
    UNION ALL
    SELECT
        HOUR,
        'ETH' AS symbol,
        'ETH' AS currency_address,
        decimals,
        price AS hourly_prices
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND HOUR :: DATE IN (
            SELECT
                block_timestamp :: DATE
            FROM
                base_table
        )
),
eth_price AS (
    SELECT
        HOUR,
        price AS eth_hourly_prices
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND HOUR :: DATE IN (
            SELECT
                block_timestamp :: DATE
            FROM
                base_table
        )
)
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
    ap.symbol AS currency_symbol,
    b.currency_address,
    CASE
        WHEN b.currency_address IN (
            'ETH',
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ) THEN total_price_raw / pow(
            10,
            18
        )
        ELSE COALESCE (total_price_raw / pow(10, decimals), total_price_raw)
    END AS price,
    IFF(
        decimals IS NULL,
        0,
        price * hourly_prices
    ) AS price_usd,
    CASE
        WHEN b.currency_address IN (
            'ETH',
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ) THEN total_fees_raw / pow(
            10,
            18
        )
        ELSE COALESCE (total_fees_raw / pow(10, decimals), total_fees_raw)
    END AS total_fees,
    IFF(
        decimals IS NULL,
        0,
        total_fees * hourly_prices
    ) AS total_fees_usd,
    CASE
        WHEN b.currency_address IN (
            'ETH',
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ) THEN platform_fee_raw / pow(
            10,
            18
        )
        ELSE COALESCE (platform_fee_raw / pow(10, decimals), platform_fee_raw)
    END AS platform_fee,
    IFF(
        decimals IS NULL,
        0,
        platform_fee * hourly_prices
    ) AS platform_fee_usd,
    CASE
        WHEN b.currency_address IN (
            'ETH',
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ) THEN creator_fee_raw / pow(
            10,
            18
        )
        ELSE COALESCE (creator_fee_raw / pow(10, decimals), creator_fee_raw)
    END AS creator_fee,
    IFF(
        decimals IS NULL,
        0,
        creator_fee * hourly_prices
    ) AS creator_fee_usd,
    tx_fee,
    tx_fee * eth_hourly_prices AS tx_fee_usd,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    input_data,
    nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    base_table b
    LEFT JOIN all_prices ap
    ON DATE_TRUNC(
        'hour',
        b.block_timestamp
    ) = ap.hour
    AND b.currency_address = ap.currency_address
    LEFT JOIN eth_price ep
    ON DATE_TRUNC(
        'hour',
        b.block_timestamp
    ) = ep.hour
