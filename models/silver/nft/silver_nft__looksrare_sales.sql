{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH looksrare_sales AS (

    SELECT
        _log_id,
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        ingested_at,
        contract_address,
        event_name,
        event_inputs :amount :: INTEGER AS nft_count,
        event_inputs :collection :: STRING AS collection_address,
        event_inputs :currency :: STRING AS currency_address,
        event_inputs :maker :: STRING AS maker_address,
        event_inputs :taker :: STRING AS taker_address,
        event_inputs :price :: INTEGER AS price,
        event_inputs :tokenId :: STRING AS tokenId,
        ROW_NUMBER() over(
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS agg_id
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address = '0x59728544b08ab483533076417fbbb2fd0b17ce3a'
        AND event_name IN (
            'TakerAsk',
            'TakerBid'
        )
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
nft_transfers AS (
    SELECT
        nft.tx_hash AS tx_hash,
        nft.contract_address AS nft_address,
        nft.project_name AS project_name,
        nft.from_address AS from_address,
        nft.to_address AS to_address,
        nft.tokenid AS tokenid,
        nft.token_metadata AS token_metadata,
        nft.erc1155_value AS erc1155_value,
        nft.ingested_at AS ingested_at,
        nft._log_id AS _log_id,
        nft.event_index AS event_index,
        ROW_NUMBER() over(
            PARTITION BY nft.tx_hash
            ORDER BY
                nft.event_index ASC
        ) AS agg_id
    FROM
        {{ ref('silver__nft_transfers') }} AS nft
    WHERE
        nft.tx_hash IN (
            SELECT
                tx_hash
            FROM
                looksrare_sales
        )

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
multi_transfers AS (
    SELECT
        tx_hash,
        nft_address,
        tokenID,
        COUNT(*)
    FROM
        nft_transfers
    GROUP BY
        tx_hash,
        nft_address,
        tokenID
    HAVING
        COUNT(*) > 1
),
multi_sales AS (
    SELECT
        tx_hash,
        collection_address,
        tokenId,
        COUNT(*)
    FROM
        looksrare_sales
    GROUP BY
        tx_hash,
        collection_address,
        tokenId
    HAVING
        COUNT(*) > 1
),
aggregator_txs AS (
    SELECT
        *
    FROM
        multi_transfers
    WHERE
        tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                multi_sales
        )
),
address_logic AS (
    SELECT
        *,
        LAST_VALUE(to_address) over (
            PARTITION BY tx_hash,
            nft_address,
            tokenId
            ORDER BY
                event_index ASC
        ) AS buyer,
        FIRST_VALUE(from_address) over (
            PARTITION BY tx_hash,
            nft_address,
            tokenId
            ORDER BY
                event_index ASC
        ) AS seller
    FROM
        nft_transfers
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                aggregator_txs
        )
),
agg_buyer_seller AS (
    SELECT
        DISTINCT tx_hash,
        nft_address,
        tokenId,
        buyer,
        seller
    FROM
        address_logic
),
creator_fees AS (
    SELECT
        _log_id,
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        ingested_at,
        contract_address,
        event_name,
        event_inputs :amount :: INTEGER AS creator_fee_unadj,
        event_inputs :collection :: STRING AS collection_address,
        event_inputs :currency :: STRING AS currency_address,
        event_inputs :royaltyRecipient :: STRING AS royalty_recipient,
        event_inputs :tokenId :: STRING AS tokenID,
        ROW_NUMBER() over(
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS agg_id
    FROM
        {{ ref('silver__logs') }}
    WHERE
        event_name = 'RoyaltyPayment'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                looksrare_sales
        )

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
fee_sharing AS (
    SELECT
        tx_hash,
        contract_address AS currency_address,
        to_address,
        from_address,
        raw_amount AS fee_share_unadj,
        event_index,
        ROW_NUMBER() over(
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS agg_id
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                looksrare_sales
        )
        AND to_address IN (
            '0x5924a28caaf1cc016617874a2f0c3710d881f3c1',
            '0xc43eb2d8bc29da90253b8006f0f38e29bfc1369b'
        )

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
decimals AS (
    SELECT
        address,
        symbol,
        decimals
    FROM
        {{ ref('silver__contracts') }}
    WHERE
        address IN (
            SELECT
                DISTINCT LOWER(currency_address)
            FROM
                looksrare_sales
        )
),
token_prices AS (
    SELECT
        HOUR,
        CASE
            WHEN LOWER(token_address) IS NULL THEN 'ETH'
            ELSE LOWER(token_address)
        END AS token_address,
        AVG(price) AS token_price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        (
            token_address IN (
                SELECT
                    DISTINCT LOWER(currency_address)
                FROM
                    looksrare_sales
            )
            OR (
                token_address IS NULL
                AND symbol IS NULL
            )
        )
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                looksrare_sales
        )
    GROUP BY
        HOUR,
        token_address
),
eth_sales AS (
    SELECT
        tx_hash,
        'ETH' AS currency_address
    FROM
        looksrare_sales
    WHERE
        tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                fee_sharing
        )
),
trade_currency AS (
    SELECT
        tx_hash,
        currency_address
    FROM
        fee_sharing
    UNION ALL
    SELECT
        tx_hash,
        currency_address
    FROM
        eth_sales
),
tx_currency AS (
    SELECT
        DISTINCT tx_hash,
        currency_address
    FROM
        trade_currency
),
tx_data AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_number,
        to_address,
        from_address,
        eth_value,
        tx_fee,
        origin_function_signature,
        CASE
            WHEN to_address = '0x59728544b08ab483533076417fbbb2fd0b17ce3a' THEN 'DIRECT'
            ELSE 'INDIRECT'
        END AS interaction_type,
        ingested_at,
        input_data 
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                looksrare_sales
        )

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
eth_prices AS (
    SELECT
        HOUR,
        token_address,
        token_price AS eth_price
    FROM
        token_prices
    WHERE
        token_address = 'ETH'
),
FINAL AS (
    SELECT
        looksrare_sales._log_id AS _log_id,
        looksrare_sales.block_number AS block_number,
        looksrare_sales.block_timestamp AS block_timestamp,
        looksrare_sales.tx_hash AS tx_hash,
        looksrare_sales.ingested_at AS ingested_at,
        looksrare_sales.contract_address AS platform_address,
        CASE
            WHEN looksrare_sales.maker_address = nft_transfers.to_address THEN 'bid_won'
            ELSE 'sale'
        END AS event_type,
        'looksrare' AS platform_name,
        looksrare_sales.event_name AS event_name,
        looksrare_sales.nft_count AS nft_count,
        looksrare_sales.collection_address AS nft_address,
        looksrare_sales.currency_address AS currency_address,
        looksrare_sales.maker_address AS maker_address,
        looksrare_sales.taker_address AS taker_address,
        looksrare_sales.price AS unadj_price,
        looksrare_sales.tokenId AS tokenId,
        CASE
            WHEN looksrare_sales.tx_hash IN (
                SELECT
                    tx_hash
                FROM
                    agg_buyer_seller
            ) THEN agg_buyer_seller.seller
            ELSE nft_transfers.from_address
        END AS nft_from_address,
        CASE
            WHEN looksrare_sales.tx_hash IN (
                SELECT
                    tx_hash
                FROM
                    agg_buyer_seller
            ) THEN agg_buyer_seller.buyer
            ELSE nft_transfers.to_address
        END AS nft_to_address,
        nft_transfers.erc1155_value AS erc1155_value,
        nft_transfers.project_name AS project_name,
        tx_data.tx_fee AS tx_fee,
        ROUND(
            tx_fee * eth_price,
            2
        ) AS tx_fee_usd,
        CASE
            WHEN tx_currency.currency_address = 'ETH' THEN 'ETH'
            ELSE symbol
        END AS currency_symbol,
        CASE
            WHEN tx_currency.currency_address = 'ETH' THEN 18
            ELSE decimals
        END AS token_decimals,
        creator_fee_unadj,
        fee_share_unadj,
        COALESCE(unadj_price / nft_count / pow(10, token_decimals), unadj_price) AS adj_price,
        COALESCE(
            creator_fee_unadj / nft_count / pow(
                10,
                token_decimals
            ),
            creator_fee_unadj,
            0
        ) AS creator_fee,
        COALESCE(
            fee_share_unadj / nft_count / pow(
                10,
                token_decimals
            ),
            fee_share_unadj,
            0
        ) AS platform_fee,
        creator_fee + platform_fee AS total_fees,
        CASE
            WHEN token_decimals IS NULL THEN NULL
            ELSE ROUND(
                adj_price * token_price,
                2
            )
        END AS price_usd,
        CASE
            WHEN token_decimals IS NULL THEN NULL
            ELSE ROUND(
                total_fees * token_price,
                2
            )
        END AS total_fees_usd,
        CASE
            WHEN token_decimals IS NULL THEN NULL
            ELSE ROUND(
                platform_fee * token_price,
                2
            )
        END AS platform_fee_usd,
        CASE
            WHEN token_decimals IS NULL THEN NULL
            ELSE ROUND(
                creator_fee * token_price,
                2
            )
        END AS creator_fee_usd,
        tx_data.to_address AS origin_to_address,
        tx_data.from_address AS origin_from_address,
        tx_data.origin_function_signature AS origin_function_signature,
        nft_transfers.token_metadata AS token_metadata,
        input_data
    FROM
        looksrare_sales
        LEFT JOIN nft_transfers
        ON looksrare_sales.tx_hash = nft_transfers.tx_hash
        AND (
            (
                looksrare_sales.maker_address = nft_transfers.from_address
                AND looksrare_sales.taker_address = nft_transfers.to_address
            )
            OR (
                looksrare_sales.maker_address = nft_transfers.to_address
                AND looksrare_sales.taker_address = nft_transfers.from_address
            )
            OR (
                looksrare_sales.taker_address = nft_transfers.to_address
            )
        )
        AND looksrare_sales.tokenId = nft_transfers.tokenId
        LEFT JOIN creator_fees
        ON creator_fees.tx_hash = looksrare_sales.tx_hash
        AND creator_fees.agg_id = looksrare_sales.agg_id
        LEFT JOIN fee_sharing
        ON fee_sharing.tx_hash = looksrare_sales.tx_hash
        AND fee_sharing.agg_id = looksrare_sales.agg_id
        LEFT JOIN tx_currency
        ON tx_currency.tx_hash = looksrare_sales.tx_hash
        LEFT JOIN token_prices
        ON token_prices.hour = DATE_TRUNC(
            'HOUR',
            looksrare_sales.block_timestamp
        )
        AND tx_currency.currency_address = token_prices.token_address
        LEFT JOIN tx_data
        ON tx_data.tx_hash = looksrare_sales.tx_hash
        LEFT JOIN eth_prices
        ON eth_prices.hour = DATE_TRUNC(
            'HOUR',
            looksrare_sales.block_timestamp
        )
        LEFT JOIN decimals
        ON tx_currency.currency_address = decimals.address
        LEFT JOIN agg_buyer_seller
        ON agg_buyer_seller.tx_hash = looksrare_sales.tx_hash
        AND agg_buyer_seller.tokenId = looksrare_sales.tokenId
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_to_address,
    origin_from_address,
    origin_function_signature,
    event_type,
    platform_address,
    platform_name,
    'looksrare' AS platform_exchange_version,
    nft_from_address,
    nft_to_address,
    nft_address,
    project_name,
    erc1155_value,
    tokenId,
    token_metadata,
    currency_symbol,
    currency_address,
    adj_price AS price,
    price_usd,
    total_fees,
    platform_fee,
    creator_fee,
    total_fees_usd,
    platform_fee_usd,
    creator_fee_usd,
    tx_fee,
    tx_fee_usd,
    _log_id,
    ingested_at,
    input_data
FROM
    FINAL
WHERE
    nft_from_address IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    ingested_at DESC)) = 1
