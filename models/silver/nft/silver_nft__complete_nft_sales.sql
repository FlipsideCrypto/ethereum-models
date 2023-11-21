{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform_name','platform_exchange_version'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
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
        erc1155_value :: STRING AS erc1155_value,
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
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
    erc1155_value :: STRING AS erc1155_value,
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
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
    erc1155_value :: STRING AS erc1155_value,
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
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
    erc1155_value :: STRING AS erc1155_value,
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
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
    erc1155_value :: STRING AS erc1155_value,
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
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
    erc1155_value :: STRING AS erc1155_value,
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
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
    erc1155_value :: STRING AS erc1155_value,
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
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
    erc1155_value :: STRING AS erc1155_value,
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
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
    erc1155_value :: STRING AS erc1155_value,
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
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
    erc1155_value :: STRING AS erc1155_value,
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
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
    erc1155_value :: STRING AS erc1155_value,
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
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
    erc1155_value :: STRING AS erc1155_value,
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
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
    erc1155_value :: STRING AS erc1155_value,
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
    {{ ref('silver_nft__blur_v2') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
prices_raw AS (
    SELECT
        HOUR,
        symbol,
        token_address,
        decimals,
        price AS hourly_prices
    FROM
        {{ ref('price__ez_hourly_token_prices') }}
    WHERE
        token_address IN (
            SELECT
                DISTINCT currency_address
            FROM
                nft_base_models
        )
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                nft_base_models
        )
),
all_prices AS (
    SELECT
        HOUR,
        symbol,
        token_address,
        decimals,
        hourly_prices
    FROM
        prices_raw
    UNION ALL
    SELECT
        HOUR,
        'ETH' AS symbol,
        'ETH' AS token_address,
        decimals,
        hourly_prices
    FROM
        prices_raw
    WHERE
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    UNION ALL
    SELECT
        HOUR,
        'ETH' AS symbol,
        '0x0000000000a39bb272e79075ade125fd351887ac' AS token_address,
        decimals,
        hourly_prices
    FROM
        prices_raw
    WHERE
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
),
eth_price AS (
    SELECT
        HOUR,
        hourly_prices AS eth_price_hourly
    FROM
        prices_raw
    WHERE
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
),
final_base AS (
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
            WHEN origin_to_address IN (
                '0x00000000005228b791a99a61f36a130d50600106'
            ) THEN 'looksrare'
            ELSE platform_name
        END AS platform_name,
        platform_exchange_version,
        --credits to hildobby and 0xRob for reservoir calldata logic https://github.com/duneanalytics/spellbook/blob/main/models/nft/ethereum/nft_ethereum_aggregators_markers.sql
        CASE
            WHEN RIGHT(
                input_data,
                2
            ) = '1f'
            AND LEFT(REGEXP_REPLACE(input_data, '^.*00', ''), 2) = '1f'
            AND REGEXP_REPLACE(
                input_data,
                '^.*00',
                ''
            ) != '1f'
            AND LENGTH(REGEXP_REPLACE(input_data, '^.*00', '')) % 2 = 0 THEN REGEXP_REPLACE(
                input_data,
                '^.*00',
                ''
            )
            ELSE NULL
        END AS calldata_hash,
        IFF(
            calldata_hash IS NULL,
            NULL,
            utils.udf_hex_to_string (
                SPLIT(
                    calldata_hash,
                    '1f'
                ) [1] :: STRING
            )
        ) AS marketplace_decoded,
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
            WHEN RIGHT(
                input_data,
                8
            ) = 'a8a9c101' THEN 'Alpha Sharks'
            WHEN RIGHT(
                input_data,
                8
            ) = '61598d6d' THEN 'Flip'
            WHEN RIGHT(
                input_data,
                15
            ) = '9616c6c64617461' THEN 'Rarible'
            WHEN marketplace_decoded IS NOT NULL THEN marketplace_decoded
            ELSE NULL
        END AS aggregator_name,
        seller_address,
        buyer_address,
        nft_address,
        C.name AS project_name,
        erc1155_value,
        tokenId,
        p.symbol AS currency_symbol,
        currency_address,
        total_price_raw,
        total_fees_raw,
        platform_fee_raw,
        creator_fee_raw,
        CASE
            WHEN currency_address IN (
                'ETH',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
                '0x0000000000a39bb272e79075ade125fd351887ac'
            ) THEN total_price_raw / pow(
                10,
                18
            )
            ELSE COALESCE (total_price_raw / pow(10, p.decimals), total_price_raw)
        END AS price,
        IFF(
            p.decimals IS NULL,
            0,
            price * hourly_prices
        ) AS price_usd,
        CASE
            WHEN currency_address IN (
                'ETH',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
                '0x0000000000a39bb272e79075ade125fd351887ac'
            ) THEN total_fees_raw / pow(
                10,
                18
            )
            ELSE COALESCE (total_fees_raw / pow(10, p.decimals), total_fees_raw)
        END AS total_fees,
        IFF(
            p.decimals IS NULL,
            0,
            total_fees * hourly_prices
        ) AS total_fees_usd,
        CASE
            WHEN currency_address IN (
                'ETH',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
                '0x0000000000a39bb272e79075ade125fd351887ac'
            ) THEN platform_fee_raw / pow(
                10,
                18
            )
            ELSE COALESCE (platform_fee_raw / pow(10, p.decimals), platform_fee_raw)
        END AS platform_fee,
        IFF(
            p.decimals IS NULL,
            0,
            platform_fee * hourly_prices
        ) AS platform_fee_usd,
        CASE
            WHEN currency_address IN (
                'ETH',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
                '0x0000000000a39bb272e79075ade125fd351887ac'
            ) THEN creator_fee_raw / pow(
                10,
                18
            )
            ELSE COALESCE (creator_fee_raw / pow(10, p.decimals), creator_fee_raw)
        END AS creator_fee,
        IFF(
            p.decimals IS NULL,
            0,
            creator_fee * hourly_prices
        ) AS creator_fee_usd,
        tx_fee,
        tx_fee * eth_price_hourly AS tx_fee_usd,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        nft_log_id,
        input_data,
        _log_id,
        b._inserted_timestamp
    FROM
        nft_base_models b
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON b.nft_address = C.address
        LEFT JOIN all_prices p
        ON DATE_TRUNC(
            'hour',
            b.block_timestamp
        ) = p.hour
        AND b.currency_address = p.token_address
        LEFT JOIN eth_price e
        ON DATE_TRUNC(
            'hour',
            b.block_timestamp
        ) = e.hour
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    calldata_hash,
    marketplace_decoded,
    aggregator_name,
    seller_address,
    buyer_address,
    nft_address,
    b.project_name,
    erc1155_value,
    tokenId,
    token_metadata,
    currency_symbol,
    currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    price,
    price_usd,
    total_fees,
    total_fees_usd,
    platform_fee,
    platform_fee_usd,
    creator_fee,
    creator_fee_usd,
    tx_fee,
    tx_fee_usd,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    nft_log_id,
    input_data,
    _log_id,
    b._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['nft_address','tokenId','platform_exchange_version','_log_id']
    ) }} AS complete_nft_sales_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    final_base b
    LEFT JOIN {{ ref('silver__nft_labels_temp') }}
    m
    ON b.nft_address = m.project_address
    AND b.tokenId = m.token_id qualify(ROW_NUMBER() over(PARTITION BY nft_log_id
ORDER BY
    b._inserted_timestamp DESC)) = 1
