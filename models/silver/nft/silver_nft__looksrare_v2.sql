{{ config(
    materialized = 'incremental',
    unique_key = 'nft_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH looksrare_fee_wallets AS (

    SELECT
        decoded_flat :protocolFeeRecipient :: STRING AS address
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_number >= 16824890
        AND contract_address = '0x0000000000e655fae4d56241588680f86e3b2377'
        AND event_name = 'NewProtocolFeeRecipient'
),
base_logs AS (
    SELECT
        block_number,
        tx_hash,
        event_index,
        event_name,
        contract_address AS platform_address,
        decoded_data,
        decoded_flat,
        decoded_flat :amounts AS amounts_array,
        -- always 1 for erc721, can be > 1 for erc1155
        CASE
            WHEN event_name = 'TakerAsk' THEN decoded_flat :askUser :: STRING
            WHEN event_name = 'TakerBid' THEN NULL
        END AS seller_address_temp,
        CASE
            WHEN event_name = 'TakerAsk' THEN decoded_flat :bidUser :: STRING
            WHEN event_name = 'TakerBid' THEN decoded_flat :bidRecipient :: STRING
        END AS buyer_address,
        decoded_flat :collection :: STRING AS nft_address,
        decoded_flat :itemIds AS tokenId_array,
        ARRAY_SIZE(
            decoded_flat :itemIds
        ) AS offer_count,
        decoded_flat :currency :: STRING AS currency_address,
        decoded_flat :feeAmounts AS amount_raw_array,
        decoded_flat :feeRecipients AS amount_recipient_array,
        decoded_flat :strategyId AS strategy,
        --https://docs.looksrare.org/guides/faqs/what-am-i-signing-when-trading-on-looksrare
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_number >= 16824890
        AND contract_address = '0x0000000000e655fae4d56241588680f86e3b2377'
        AND event_name IN (
            'TakerAsk',
            'TakerBid'
        )
),
sale_amounts AS (
    SELECT
        tx_hash,
        event_index,
        offer_count,
        amount_raw_array,
        VALUE :: FLOAT / offer_count AS raw_values,
        INDEX,
        CASE
            WHEN INDEX = 0 THEN raw_values
            ELSE 0
        END AS sale_price_raw_,
        CASE
            WHEN ARRAY_SIZE(amount_raw_array) -1 = INDEX THEN raw_values
            ELSE 0
        END AS platform_fee_raw_,
        CASE
            WHEN INDEX != 0
            AND INDEX != ARRAY_SIZE(amount_raw_array) -1 THEN raw_values
            ELSE 0
        END AS royalty_fee_raw_
    FROM
        base_logs,
        LATERAL FLATTEN (
            input => amount_raw_array
        )
),
sale_amounts_agg AS (
    SELECT
        tx_hash,
        event_index,
        SUM(sale_price_raw_) AS sale_price_raw,
        SUM(platform_fee_raw_) AS platform_fee_raw,
        SUM(royalty_fee_raw_) AS royalty_fee_raw
    FROM
        sale_amounts
    GROUP BY
        tx_hash,
        event_index
),
fee_recipients AS (
    SELECT
        tx_hash,
        event_index,
        VALUE :: STRING AS sale_amount_receiver,
        INDEX
    FROM
        base_logs,
        LATERAL FLATTEN (
            input => amount_recipient_array
        )
    WHERE
        INDEX = 0
),
token_ids AS (
    SELECT
        tx_hash,
        event_index,
        VALUE :: STRING AS tokenId,
        INDEX
    FROM
        base_logs,
        LATERAL FLATTEN (
            input => tokenId_array
        )
),
token_ids_amount AS (
    SELECT
        t.tx_hash,
        t.event_index,
        t.tokenId,
        s.sale_price_raw,
        s.platform_fee_raw,
        s.royalty_fee_raw
    FROM
        token_ids t full
        OUTER JOIN sale_amounts_agg s
        ON t.tx_hash = s.tx_hash
        AND t.event_index = s.event_index
),
final_base AS (
    SELECT
        block_number,
        t.tx_hash,
        t.event_index,
        event_name,
        platform_address,
        decoded_flat,
        amounts_array,
        COALESCE(
            seller_address_temp,
            sale_amount_receiver
        ) AS seller_address,
        buyer_address,
        nft_address,
        tokenId,
        tokenId_array,
        sale_price_raw,
        platform_fee_raw,
        royalty_fee_raw,
        platform_fee_raw + royalty_fee_raw AS total_fees_raw,
        sale_price_raw + total_fees_raw AS price_raw,
        IFF(
            currency_address = '0x0000000000000000000000000000000000000000',
            'ETH',
            currency_address
        ) AS currency_address,
        offer_count,
        amount_raw_array,
        amount_recipient_array,
        strategy,
        _log_id,
        _inserted_timestamp
    FROM
        token_ids_amount t
        INNER JOIN base_logs b
        ON b.tx_hash = t.tx_hash
        AND b.event_index = t.event_index
        LEFT JOIN fee_recipients f
        ON t.tx_hash = f.tx_hash
        AND t.event_index = f.event_index
),
tx_data AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_number,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        origin_function_signature,
        tx_fee,
        input_data
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        block_timestamp :: DATE >= '2023-03-01'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                final_base
        )
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
        (
            currency_address IN (
                SELECT
                    DISTINCT currency_address
                FROM
                    final_base
            )
        )
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
        'ETH' AS currency_address,
        decimals,
        price AS hourly_prices
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                tx_data
        )
        AND HOUR :: DATE >= '2023-03-01'
),
eth_price AS (
    SELECT
        HOUR,
        (price) AS eth_price_hourly
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        HOUR :: DATE >= '2023-03-01'
        AND token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                tx_data
        )
),
nft_transfers AS (
    SELECT
        tx_hash,
        event_index,
        contract_address,
        tokenId,
        erc1155_value
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        block_timestamp :: DATE >= '2023-03-01'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                final_base
        )
),
final_base_tx AS (
    SELECT
        b.block_number,
        t.block_timestamp,
        b.tx_hash,
        b.event_index,
        event_name,
        platform_address,
        decoded_flat,
        amounts_array,
        seller_address,
        buyer_address,
        b.nft_address,
        b.tokenId,
        tokenId_array,
        price_raw,
        total_fees_raw,
        platform_fee_raw,
        royalty_fee_raw AS creator_fee_raw,
        b.currency_address,
        offer_count,
        amount_raw_array,
        amount_recipient_array,
        strategy,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_fee,
        input_data,
        _log_id,
        _inserted_timestamp
    FROM
        final_base b
        INNER JOIN tx_data t
        ON b.tx_hash = t.tx_hash
)
SELECT
    b.block_number,
    b.block_timestamp,
    b.tx_hash,
    b.event_index,
    event_name,
    IFF(
        event_name = 'TakerBid',
        'sale',
        'bid_won'
    ) AS event_type,
    'looksrare' AS platform_name,
    platform_address,
    'looksrare v2' AS platform_exchange_version,
    decoded_flat,
    amounts_array,
    seller_address,
    buyer_address,
    b.nft_address,
    b.tokenId,
    n.erc1155_value,
    tokenId_array,
    price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    CASE
        WHEN b.currency_address IN (
            'ETH',
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ) THEN price_raw / pow(
            10,
            18
        )
        ELSE COALESCE (price_raw / pow(10, decimals), price_raw)
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
    b.currency_address,
    ap.symbol AS currency_symbol,
    offer_count,
    amount_raw_array,
    amount_recipient_array,
    strategy,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_fee,
    tx_fee * eth_price_hourly AS tx_fee_usd,
    input_data,
    CONCAT(
        b.nft_address,
        '-',
        b.tokenId,
        '-',
        _log_id,
        '-',
        platform_exchange_version
    ) AS nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    final_base_tx b
    LEFT JOIN all_prices ap
    ON ap.hour = DATE_TRUNC(
        'hour',
        b.block_timestamp
    )
    AND ap.currency_address = b.currency_address
    LEFT JOIN eth_price ep
    ON ep.hour = DATE_TRUNC(
        'hour',
        b.block_timestamp
    )
    LEFT JOIN nft_transfers n
    ON b.tx_hash = n.tx_hash
    AND b.nft_address = n.contract_address
    AND b.tokenId = n.tokenId qualify ROW_NUMBER() over (
        PARTITION BY b.tx_hash,
        nft_log_id
        ORDER BY
            block_timestamp ASC
    ) = 1
