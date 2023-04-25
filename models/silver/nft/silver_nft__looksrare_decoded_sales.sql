{{ config(
    materialized = 'incremental',
    unique_key = 'nft_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base_decoded AS (

    SELECT
        block_number,
        tx_hash,
        contract_address,
        event_index,
        event_name,
        decoded_data,
        decoded_flat,
        decoded_flat :maker :: STRING AS maker,
        decoded_flat :taker :: STRING AS taker,
        CASE
            WHEN event_name = 'TakerAsk' THEN taker
            WHEN event_name = 'TakerBid' THEN maker
        END AS seller_address,
        CASE
            WHEN event_name = 'TakerAsk' THEN maker
            WHEN event_name = 'TakerBid' THEN taker
        END AS buyer_address,
        decoded_flat :collection :: STRING AS nft_address,
        decoded_flat :tokenId :: STRING AS tokenId,
        decoded_flat :amount :: INT AS nft_tokenid_quantity,
        decoded_flat :currency :: STRING AS currency_address,
        decoded_flat :orderHash :: STRING AS orderhash,
        decoded_flat :price :: INT AS total_price_raw,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS tx_hash_base_identifier,
        CONCAT(
            tx_hash,
            '-',
            nft_address,
            '-',
            tokenId,
            '-',
            tx_hash_base_identifier
        ) AS tx_hash_identifier,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_number >= 13885625
        AND contract_address = '0x59728544b08ab483533076417fbbb2fd0b17ce3a'
        AND event_name IN (
            'TakerAsk',
            'TakerBid'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
),
royalty_fee_transfers AS (
    SELECT
        tx_hash,
        decoded_flat :amount :: INT AS royalty_fee_raw_,
        decoded_flat :collection :: STRING AS nft_address,
        decoded_flat :tokenId :: STRING AS tokenId,
        decoded_flat :currency :: STRING AS royalty_fee_currency_address,
        decoded_flat :royaltyRecipient :: STRING AS royalty_recipient,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS tx_hash_royalty_identifier
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_number >= 13885625
        AND contract_address = '0x59728544b08ab483533076417fbbb2fd0b17ce3a'
        AND event_name = 'RoyaltyPayment'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                base_decoded
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
),
platform_fee_transfers AS (
    SELECT
        tx_hash,
        from_address AS platform_fee_from_address,
        to_address AS platform_fee_to_address,
        contract_address AS platform_fee_currency_address,
        raw_amount AS platform_fee_raw_,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS tx_hash_platform_fee_identifier
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        block_number >= 13885625
        AND to_address IN (
            -- looksrare fee wallets
            '0x5924a28caaf1cc016617874a2f0c3710d881f3c1',
            '0xc43eb2d8bc29da90253b8006f0f38e29bfc1369b'
        )
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                base_decoded
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
),
base_decoded_combined AS (
    SELECT
        block_number,
        b.tx_hash,
        contract_address,
        event_index,
        event_name,
        decoded_data,
        decoded_flat,
        maker,
        taker,
        seller_address,
        buyer_address,
        b.nft_address,
        b.tokenId,
        nft_tokenid_quantity,
        b.currency_address,
        orderhash,
        total_price_raw,
        COALESCE(
            royalty_fee_raw_,
            0
        ) AS royalty_fee_raw,
        royalty_fee_currency_address,
        royalty_recipient,
        platform_fee_from_address,
        platform_fee_to_address,
        platform_fee_currency_address,
        COALESCE(
            platform_fee_raw_,
            0
        ) AS platform_fee_raw,
        royalty_fee_raw + platform_fee_raw AS total_fees_raw,
        tx_hash_platform_fee_identifier,
        tx_hash_base_identifier,
        tx_hash_identifier,
        _log_id,
        _inserted_timestamp
    FROM
        base_decoded b
        LEFT JOIN royalty_fee_transfers r
        ON b.tx_hash = r.tx_hash
        AND b.tx_hash_base_identifier = r.tx_hash_royalty_identifier
        LEFT JOIN platform_fee_transfers p
        ON b.tx_hash = p.tx_hash
        AND b.tx_hash_base_identifier = p.tx_hash_platform_fee_identifier
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
        block_timestamp :: DATE >= '2021-12-20'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                base_decoded_combined
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
),
all_prices AS (
    SELECT
        HOUR,
        symbol,
        token_address AS currency_address,
        decimals,
        (price) AS hourly_prices
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        (
            currency_address IN (
                SELECT
                    DISTINCT currency_address
                FROM
                    base_decoded_combined
            )
        )
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                tx_data
        )
        AND HOUR :: DATE >= '2021-12-20'
    UNION ALL
    SELECT
        HOUR,
        'ETH' AS symbol,
        'ETH' AS currency_address,
        decimals,
        (price) AS hourly_prices
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
        AND HOUR :: DATE >= '2021-12-20'
),
eth_price AS (
    SELECT
        HOUR,
        (price) AS eth_price_hourly
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        HOUR :: DATE >= '2021-12-20'
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
        erc1155_value,
        CONCAT(
            tx_hash,
            '-',
            contract_address,
            '-',
            tokenId
        ) AS nft_id
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        block_timestamp :: DATE >= '2021-12-20'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                base_decoded_combined
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
),
FINAL AS (
    SELECT
        t.block_timestamp,
        b.block_number,
        b.tx_hash,
        b.contract_address AS platform_address,
        'looksrare' AS platform_name,
        'looksrare' AS platform_exchange_version,
        b.event_index,
        b.event_name,
        CASE
            WHEN b.event_name = 'TakerBid' THEN 'sale'
            WHEN b.event_name = 'TakerAsk' THEN 'bid_won'
        END AS event_type,
        decoded_data,
        decoded_flat,
        maker,
        taker,
        seller_address,
        buyer_address,
        b.nft_address,
        b.tokenId,
        nft_tokenid_quantity,
        n.erc1155_value,
        b.currency_address,
        p.symbol AS currency_symbol,
        orderhash,
        total_price_raw,
        royalty_fee_raw AS creator_fee_raw,
        platform_fee_raw,
        total_fees_raw,
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
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        t.tx_fee,
        t.tx_fee * eth_price_hourly AS tx_fee_usd,
        input_data,
        tx_hash_identifier,
        _log_id,
        CONCAT(
            b.nft_address,
            '-',
            b.tokenId,
            '-',
            platform_exchange_version,
            '-',
            _log_id
        ) AS nft_log_id,
        _inserted_timestamp
    FROM
        base_decoded_combined b
        INNER JOIN tx_data t
        ON b.tx_hash = t.tx_hash
        LEFT JOIN nft_transfers n
        ON n.tx_hash = b.tx_hash
        AND n.contract_address = b.nft_address
        AND n.tokenId = b.tokenId
        LEFT JOIN all_prices p
        ON DATE_TRUNC(
            'hour',
            t.block_timestamp
        ) = p.hour
        AND b.currency_address = p.currency_address
        LEFT JOIN eth_price e
        ON DATE_TRUNC(
            'hour',
            t.block_timestamp
        ) = e.hour qualify(ROW_NUMBER() over(PARTITION BY nft_log_id
    ORDER BY
        _inserted_timestamp DESC)) = 1
)
SELECT
    *
FROM
    FINAL
