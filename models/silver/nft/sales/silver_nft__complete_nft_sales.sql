{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform_exchange_version'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg', 'heal']
) }}

WITH nft_base_models AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
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

{% if is_incremental() and 'wyvern' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
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

{% if is_incremental() and 'looksrare' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
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

{% if is_incremental() and 'rarible' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
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

{% if is_incremental() and 'x2y2' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
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

{% if is_incremental() and 'nftx' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
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

{% if is_incremental() and 'seaport_1_1' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
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

{% if is_incremental() and 'cryptopunk' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
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

{% if is_incremental() and 'sudoswap' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
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

{% if is_incremental() and 'blur' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
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

{% if is_incremental() and 'seaport_1_4' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
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

{% if is_incremental() and 'looksrare_v2' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
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

{% if is_incremental() and 'seaport_1_5' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
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

{% if is_incremental() and 'blur_v2' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
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
    {{ ref('silver_nft__blur_blend_sales') }}

{% if is_incremental() and 'blur_blend' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
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
    {{ ref('silver_nft__artblocks_sales') }}

{% if is_incremental() and 'artblocks' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
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
    {{ ref('silver_nft__magiceden_sales') }}

{% if is_incremental() and 'magiceden' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
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
    {{ ref('silver_nft__seaport_1_6_sales') }}

{% if is_incremental() and 'seaport_1_6' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
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
    {{ ref('silver_nft__rarible_v2_sales') }}

{% if is_incremental() and 'rarible_v2' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
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
        {{ ref('price__ez_prices_hourly') }}
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
contracts_decimal AS (
    SELECT
        address AS address_contracts,
        symbol AS symbol_contracts,
        decimals AS decimals_contracts
    FROM
        {{ ref('silver__contracts') }}
    WHERE
        address IN (
            SELECT
                currency_address
            FROM
                nft_base_models
        )
),
final_base AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_type,
        platform_address,
        platform_name,
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
            WHEN marketplace_decoded IS NOT NULL THEN marketplace_decoded
            ELSE NULL
        END AS aggregator_name,
        seller_address,
        buyer_address,
        nft_address,
        erc1155_value,
        tokenId,
        COALESCE(
            p.symbol,
            symbol_contracts
        ) AS currency_symbol,
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
            ELSE COALESCE (
                total_price_raw / pow(10, COALESCE(p.decimals, decimals_contracts)),
                total_price_raw
            )
        END AS price,
        IFF(
            COALESCE(
                p.decimals,
                decimals_contracts
            ) IS NULL,
            0,
            price * COALESCE(
                hourly_prices,
                0
            )
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
            ELSE COALESCE (
                total_fees_raw / pow(10, COALESCE(p.decimals, decimals_contracts)),
                total_fees_raw
            )
        END AS total_fees,
        IFF(
            COALESCE(
                p.decimals,
                decimals_contracts
            ) IS NULL,
            0,
            total_fees * COALESCE(
                hourly_prices,
                0
            )
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
            ELSE COALESCE (
                platform_fee_raw / pow(10, COALESCE(p.decimals, decimals_contracts)),
                platform_fee_raw
            )
        END AS platform_fee,
        IFF(
            COALESCE(
                p.decimals,
                decimals_contracts
            ) IS NULL,
            0,
            platform_fee * COALESCE(
                hourly_prices,
                0
            )
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
            ELSE COALESCE (
                creator_fee_raw / pow(10, COALESCE(p.decimals, decimals_contracts)),
                creator_fee_raw
            )
        END AS creator_fee,
        IFF(
            COALESCE(
                p.decimals,
                decimals_contracts
            ) IS NULL,
            0,
            creator_fee * COALESCE(
                hourly_prices,
                0
            )
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
        LEFT JOIN contracts_decimal C
        ON b.currency_address = C.address_contracts
),

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
heal_model AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_type,
        platform_address,
        COALESCE(
            a2.aggregator,
            platform_name
        ) AS platform_name,
        platform_exchange_version,
        calldata_hash,
        marketplace_decoded,
        COALESCE(
            aggregator_name,
            A.aggregator
        ) AS aggregator_name,
        seller_address,
        buyer_address,
        nft_address,
        C.name AS project_name,
        erc1155_value,
        tokenId,
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
        t._inserted_timestamp
    FROM
        {{ this }}
        t
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON t.nft_address = C.address
        LEFT JOIN {{ ref('silver_nft__aggregator_list') }} A
        ON RIGHT(
            t.input_data,
            8
        ) = A.aggregator_identifier
        AND aggregator_type = 'calldata'
        LEFT JOIN {{ ref('silver_nft__aggregator_list') }}
        a2
        ON t.origin_to_address = a2.aggregator_identifier
        AND a2.aggregator_type = 'router'
    WHERE
        (
            t.block_number IN (
                SELECT
                    DISTINCT t1.block_number AS block_number
                FROM
                    {{ this }}
                    t1
                WHERE
                    t1.project_name IS NULL
                    AND _inserted_timestamp < (
                        SELECT
                            MAX(
                                _inserted_timestamp
                            ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
                        FROM
                            {{ this }}
                    )
                    AND EXISTS (
                        SELECT
                            1
                        FROM
                            {{ ref('silver__contracts') }} C
                        WHERE
                            C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                            AND C.name IS NOT NULL
                            AND C.address = t1.nft_address)
                    )
            )
            OR (
                t.block_number IN (
                    SELECT
                        DISTINCT t1.block_number AS block_number
                    FROM
                        {{ this }}
                        t1
                    WHERE
                        t1.aggregator_name IS NULL
                        AND _inserted_timestamp < (
                            SELECT
                                MAX(
                                    _inserted_timestamp
                                ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
                            FROM
                                {{ this }}
                        )
                        AND EXISTS (
                            SELECT
                                1
                            FROM
                                {{ ref('silver_nft__aggregator_list') }} A
                            WHERE
                                A._inserted_timestamp > DATEADD('DAY', -2, SYSDATE())
                                AND A.aggregator_type = 'calldata'
                                AND RIGHT(
                                    t1.input_data,
                                    8
                                ) = A.aggregator_identifier
                        )
                )
            )
            OR (
                t.block_number IN (
                    SELECT
                        DISTINCT t1.block_number AS block_number
                    FROM
                        {{ this }}
                        t1
                    WHERE
                        t1.origin_to_address IN (
                            SELECT
                                aggregator_identifier
                            FROM
                                {{ ref('silver_nft__aggregator_list') }}
                            WHERE
                                aggregator_type = 'router'
                                AND _inserted_timestamp >= DATEADD('DAY', -2, SYSDATE()))
                                AND _inserted_timestamp < (
                                    SELECT
                                        MAX(
                                            _inserted_timestamp
                                        ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
                                    FROM
                                        {{ this }}
                                )
                                AND EXISTS (
                                    SELECT
                                        1
                                    FROM
                                        {{ ref('silver_nft__aggregator_list') }}
                                        a2
                                    WHERE
                                        a2._inserted_timestamp > DATEADD('DAY', -2, SYSDATE())
                                        AND t1.origin_to_address = a2.aggregator_identifier
                                        AND a2.aggregator_type = 'router')
                                )
                        )
                ),
                {% endif %}

                combined AS (
                    SELECT
                        block_number,
                        block_timestamp,
                        tx_hash,
                        event_index,
                        event_type,
                        platform_address,
                        COALESCE(
                            a2.aggregator,
                            platform_name
                        ) AS platform_name,
                        platform_exchange_version,
                        calldata_hash,
                        marketplace_decoded,
                        COALESCE(
                            aggregator_name,
                            A.aggregator
                        ) AS aggregator_name,
                        seller_address,
                        buyer_address,
                        nft_address,
                        C.name AS project_name,
                        erc1155_value,
                        tokenId,
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
                            ['tx_hash', 'event_index', 'nft_address','tokenId','platform_exchange_version']
                        ) }} AS complete_nft_sales_id,
                        SYSDATE() AS inserted_timestamp,
                        SYSDATE() AS modified_timestamp,
                        '{{ invocation_id }}' AS _invocation_id
                    FROM
                        final_base b
                        LEFT JOIN {{ ref('silver__contracts') }} C
                        ON b.nft_address = C.address
                        LEFT JOIN {{ ref('silver_nft__aggregator_list') }} A
                        ON RIGHT(
                            b.input_data,
                            8
                        ) = A.aggregator_identifier
                        AND A.aggregator_type = 'calldata'
                        LEFT JOIN {{ ref('silver_nft__aggregator_list') }}
                        a2
                        ON b.origin_to_address = a2.aggregator_identifier
                        AND a2.aggregator_type = 'router'

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
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
    project_name,
    erc1155_value,
    tokenId,
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
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index', 'nft_address','tokenId','platform_exchange_version']
    ) }} AS complete_nft_sales_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    heal_model
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    calldata_hash,
    marketplace_decoded,
    CASE
        WHEN aggregator_name = 'Gem'
        AND block_timestamp :: DATE <= '2023-04-04' THEN 'Gem'
        WHEN aggregator_name = 'Gem'
        AND block_timestamp :: DATE >= '2023-04-05' THEN 'OpenSea Pro'
        ELSE aggregator_name
    END AS aggregator_name,
    seller_address,
    buyer_address,
    nft_address,
    project_name,
    erc1155_value,
    tokenId,
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
    _inserted_timestamp,
    complete_nft_sales_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
    combined qualify (ROW_NUMBER() over(PARTITION BY nft_log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
