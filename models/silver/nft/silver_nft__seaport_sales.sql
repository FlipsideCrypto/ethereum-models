{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH seaport_interactions AS (

    SELECT
        DISTINCT tx_hash
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_number > 14000000
        AND contract_address = '0x00000000006c3852cbef3e08e8df289169ede581'
        AND topics [0] :: STRING = '0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31'

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
seaport_traces AS (
    SELECT
        tx_hash,
        block_timestamp,
        to_address AS nft_address,
        CONCAT('0x', SUBSTR(input, 35, 40)) AS nft_from_address,
        CONCAT('0x', SUBSTR(input, 99, 40)) AS nft_to_address,
        udf_hex_to_int(SUBSTR(input, 139, 64)) AS tokenid
    FROM
        {{ ref('silver__traces') }}
    WHERE
        tx_status = 'SUCCESS'
        AND block_number > 14000000
        AND LEFT(
            input,
            10
        ) IN (
            '0x23b872dd',
            '0xf242432a'
        )
        AND to_address NOT IN (
            SELECT
                DISTINCT contract_address
            FROM
                {{ ref('silver__transfers') }}
        )
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                seaport_interactions
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
nft_transfers AS (
    SELECT
        tx_hash,
        contract_address AS nft_address,
        project_name,
        from_address,
        to_address,
        tokenid,
        token_metadata,
        erc1155_value,
        ingested_at,
        _log_id,
        event_index
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        block_number > 14000000
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                seaport_interactions
        )
        AND to_address <> '0x0000000000000000000000000000000000000000'

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
all_relevant_transfers AS (
    SELECT
        A.*,
        ROW_NUMBER() over(
            PARTITION BY A.tx_hash
            ORDER BY
                A.event_index ASC
        ) AS agg_id
    FROM
        nft_transfers A
        INNER JOIN seaport_traces b
        ON A.tx_hash = b.tx_hash
        AND A.nft_address = b.nft_address
        AND A.tokenid = b.tokenid
        AND A.from_address = b.nft_from_address
        AND A.to_address = b.nft_to_address
),
other_exchanges AS (
    SELECT
        tx_hash,
        nft_address,
        tokenId
    FROM
        {{ ref('silver_nft__looksrare_sales') }}
    UNION ALL
    SELECT
        tx_hash,
        nft_address,
        tokenId
    FROM
        {{ ref('silver_nft__nftx_sales') }}
    UNION ALL
    SELECT
        tx_hash,
        nft_address,
        tokenId
    FROM
        {{ ref('silver_nft__opensea_sales') }}
    UNION ALL
    SELECT
        tx_hash,
        nft_address,
        tokenId
    FROM
        {{ ref('silver_nft__rarible_sales') }}
    UNION ALL
    SELECT
        tx_hash,
        nft_address,
        tokenId
    FROM
        {{ ref('silver_nft__x2y2_sales') }}
),
filter_transfers AS (
    SELECT
        A.*,
        b.tx_hash AS checked
    FROM
        all_relevant_transfers A
        LEFT OUTER JOIN other_exchanges b
        ON A.tx_hash = b.tx_hash
        AND A.nft_address = b.nft_address
        AND A.tokenId = b.tokenId
),
relevant_transfers AS (
    SELECT
        *
    FROM
        filter_transfers
    WHERE
        checked IS NULL
),
eth_tx_data AS (
    SELECT
        A.tx_hash,
        A.from_address,
        A.to_address,
        A.eth_value,
        CASE
            WHEN A.to_address IN (
                '0x5b3256965e7c3cf26e11fcaf296dfc8807c01073',
                '0x8de9c5a032463c561423387a9648c5c7bcc5bc90',
                '0x0000a26b00c1f0df003000390027140000faa719'
            ) THEN 'os_fee' --fee managment contract
            WHEN A.to_address = b.from_address THEN 'to_seller'
            ELSE 'royalty'
        END AS payment_type,
        'ETH' AS currency_symbol,
        'ETH' AS currency_address
    FROM
        {{ ref('silver__eth_transfers') }} A full
        OUTER JOIN (
            SELECT
                DISTINCT tx_hash,
                from_address
            FROM
                relevant_transfers
        ) b
        ON A.tx_hash = b.tx_hash
        AND A.to_address = b.from_address
    WHERE
        block_number > 14000000
        AND A.tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                seaport_interactions
        )
        AND A.from_address = '0x00000000006c3852cbef3e08e8df289169ede581' --exchange contract
        AND A.to_address <> '0x83c8f28c26bf6aaca652df1dbbe0e1b56f8baba2'

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
count_type AS (
    SELECT
        tx_hash,
        SUM(
            CASE
                WHEN payment_type = 'os_fee' THEN 1
            END
        ) AS os_fee_count,
        SUM(
            CASE
                WHEN payment_type = 'royalty' THEN 1
            END
        ) AS royalty_count,
        SUM(
            CASE
                WHEN payment_type = 'to_seller' THEN 1
            END
        ) AS seller_payment_count
    FROM
        eth_tx_data
    GROUP BY
        tx_hash
),
nft_count AS (
    SELECT
        tx_hash,
        from_address,
        COUNT(
            DISTINCT tokenid
        ) AS nft_count
    FROM
        relevant_transfers
    GROUP BY
        tx_hash,
        from_address
),
nft_count_total AS (
    SELECT
        tx_hash,
        SUM(nft_count) AS nft_count_total
    FROM
        nft_count
    GROUP BY
        tx_hash
),
os_total_eth_fees AS (
    SELECT
        tx_hash,
        SUM(eth_value) AS amount
    FROM
        eth_tx_data
    WHERE
        payment_type = 'os_fee'
    GROUP BY
        tx_hash
),
os_fees_per_nft AS (
    SELECT
        A.tx_hash,
        amount / b.nft_count_total AS os_fees_per_nft
    FROM
        os_total_eth_fees A
        LEFT JOIN nft_count_total b
        ON A.tx_hash = b.tx_hash
),
eth_sales AS (
    SELECT
        tx_hash,
        to_address,
        SUM(eth_value) AS eth_sale_amount
    FROM
        eth_tx_data
    WHERE
        payment_type = 'to_seller'
    GROUP BY
        tx_hash,
        to_address
),
eth_sales_per AS (
    SELECT
        A.tx_hash,
        A.to_address,
        A.eth_sale_amount / b.nft_count AS eth_sale_per
    FROM
        eth_sales A
        LEFT JOIN nft_count b
        ON A.tx_hash = b.tx_hash
        AND A.to_address = b.from_address
),
eth_royalties_total AS (
    SELECT
        tx_hash,
        SUM(eth_value) AS royalty_amount
    FROM
        eth_tx_data
    WHERE
        payment_type = 'royalty'
    GROUP BY
        tx_hash
),
eth_royalties AS (
    SELECT
        A.tx_hash,
        royalty_amount / b.nft_count_total AS royalty_amount
    FROM
        eth_royalties_total A
        LEFT JOIN nft_count_total b
        ON A.tx_hash = b.tx_hash
),
token_tx_data1 AS (
    SELECT
        A.tx_hash,
        A.contract_address AS currency_address,
        A.to_address,
        A.from_address,
        CASE
            WHEN A.to_address IN (
                '0x5b3256965e7c3cf26e11fcaf296dfc8807c01073',
                '0x8de9c5a032463c561423387a9648c5c7bcc5bc90',
                '0x0000a26b00c1f0df003000390027140000faa719'
            ) THEN 'os_fee' --fee managment contract
            WHEN A.to_address = b.from_address
            OR raw_amount = MAX(raw_amount) over (
                PARTITION BY A.tx_hash
            ) THEN 'to_seller'
            ELSE 'royalty'
        END AS payment_type,
        A.raw_amount,
        A.event_index,
        ROW_NUMBER() over(
            PARTITION BY A.tx_hash
            ORDER BY
                event_index ASC
        ) AS agg_id
    FROM
        {{ ref('silver__transfers') }} A full
        OUTER JOIN (
            SELECT
                DISTINCT tx_hash,
                from_address
            FROM
                relevant_transfers
        ) b
        ON A.tx_hash = b.tx_hash
        AND A.to_address = b.from_address
    WHERE
        A.tx_hash IN (
            SELECT
                tx_hash
            FROM
                seaport_interactions
        )

{% if is_incremental() %}
AND A.ingested_at >= (
    SELECT
        MAX(
            ingested_at
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
token_tx_buyers AS (
    SELECT
        DISTINCT tx_hash,
        from_address AS buyer_address
    FROM
        token_tx_data1
    WHERE
        payment_type = 'to_seller'
),
token_tx_data AS (
    SELECT
        A.*,
        CASE
            WHEN b.buyer_address IS NOT NULL THEN 'sale'
        END AS payment_type2
    FROM
        token_tx_data1 A
        LEFT JOIN token_tx_buyers b
        ON A.tx_hash = b.tx_hash
        AND A.from_address = b.buyer_address
),
trade_currency AS (
    SELECT
        tx_hash,
        currency_address,
        CASE
            WHEN currency_address IN (
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
                '0x4d224452801aced8b2f0aebe155379bb5d594381',
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
                '0x6b175474e89094c44da98b954eedeac495271d0f'
            ) THEN 2
            ELSE 3
        END AS priority
    FROM
        token_tx_data
    UNION ALL
    SELECT
        tx_hash,
        currency_address,
        1 AS priority
    FROM
        eth_tx_data
),
tx_currency AS (
    SELECT
        DISTINCT tx_hash,
        currency_address,
        priority
    FROM
        trade_currency qualify(ROW_NUMBER() over(PARTITION BY tx_hash
    ORDER BY
        priority ASC)) = 1
),
os_total_token_fees AS (
    SELECT
        A.tx_hash,
        SUM(raw_amount) AS os_token_fees
    FROM
        token_tx_data A
        INNER JOIN tx_currency b
        ON A.tx_hash = b.tx_hash
        AND A.currency_address = b.currency_address
    WHERE
        payment_type = 'os_fee'
    GROUP BY
        1
),
os_token_fees_per AS (
    SELECT
        A.tx_hash,
        os_token_fees / nft_count_total AS os_token_fees
    FROM
        os_total_token_fees A
        LEFT JOIN nft_count_total b
        ON A.tx_hash = b.tx_hash
),
total_token_royalties AS (
    SELECT
        A.tx_hash,
        SUM(raw_amount) AS royalties
    FROM
        token_tx_data A
        INNER JOIN tx_currency b
        ON A.tx_hash = b.tx_hash
        AND A.currency_address = b.currency_address
    WHERE
        payment_type = 'royalty'
    GROUP BY
        1
),
token_royalties AS (
    SELECT
        A.tx_hash,
        royalties / nft_count_total AS royalty_fee
    FROM
        total_token_royalties A
        LEFT JOIN nft_count_total b
        ON A.tx_hash = b.tx_hash
),
total_tokens_to_seller AS (
    SELECT
        A.tx_hash,
        SUM(raw_amount) AS sale_amount
    FROM
        token_tx_data A
        INNER JOIN tx_currency b
        ON A.tx_hash = b.tx_hash
        AND A.currency_address = b.currency_address
    WHERE
        payment_type = 'to_seller'
        OR payment_type2 = 'sale'
    GROUP BY
        1
),
tokens_to_seller AS (
    SELECT
        A.tx_hash,
        sale_amount / nft_count_total AS token_sale_amount
    FROM
        total_tokens_to_seller A
        LEFT JOIN nft_count_total b
        ON A.tx_hash = b.tx_hash
),
eth_tx_sales AS (
    SELECT
        tx_hash,
        'ETH' AS currency_address
    FROM
        seaport_interactions
    WHERE
        tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                token_tx_data
        )
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
                tx_currency
        )
),
token_prices AS (
    SELECT
        HOUR,
        CASE
            WHEN LOWER(token_address) IS NULL THEN 'ETH'
            ELSE LOWER(token_address)
        END AS token_address,
        AVG(price) AS price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        (
            token_address IN (
                SELECT
                    DISTINCT LOWER(currency_address)
                FROM
                    trade_currency
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
                seaport_traces
        )
    GROUP BY
        HOUR,
        token_address
),
tx_data AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_number,
        to_address AS origin_to_address,
        from_address AS origin_from_address,
        eth_value,
        tx_fee,
        origin_function_signature,
        ingested_at
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                seaport_traces
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
        price AS eth_price
    FROM
        token_prices
    WHERE
        token_address = 'ETH'
),
FINAL AS (
    SELECT
        b.block_number,
        b.block_timestamp,
        A.tx_hash,
        b.origin_to_address,
        b.origin_from_address,
        b.origin_function_signature,
        'sale' AS event_type,
        '0x00000000006c3852cbef3e08e8df289169ede581' AS platform_address,
        'opensea' AS platform_name,
        'seaport_1_1' AS platform_exchange_version,
        A.nft_address,
        A.project_name,
        A.from_address AS seller_address,
        A.to_address AS buyer_address,
        C.currency_address AS currency_address,
        CASE
            WHEN C.currency_address = 'ETH' THEN 'ETH'
            ELSE d.symbol
        END AS currency_symbol,
        A.tokenid,
        A.token_metadata,
        A.erc1155_value,
        A.ingested_at,
        A._log_id,
        COALESCE(
            os_fees_per_nft,
            os_token_fees,
            0
        ) AS platform_fee_unadj,
        COALESCE(
            royalty_amount,
            royalty_fee,
            0
        ) AS creator_fee_unadj,
        COALESCE(
            eth_sale_per,
            token_sale_amount
        ) AS sale_amount_unadj,
        CASE
            WHEN currency_address = 'ETH' THEN platform_fee_unadj
            WHEN currency_address <> 'ETH'
            AND d.decimals IS NULL THEN platform_fee_unadj
            ELSE platform_fee_unadj / pow(
                10,
                d.decimals
            )
        END AS platform_fee,
        CASE
            WHEN currency_address = 'ETH' THEN creator_fee_unadj
            WHEN currency_address <> 'ETH'
            AND d.decimals IS NULL THEN creator_fee_unadj
            ELSE creator_fee_unadj / pow(
                10,
                d.decimals
            )
        END AS creator_fee,
        CASE
            WHEN currency_address = 'ETH' THEN sale_amount_unadj
            WHEN currency_address <> 'ETH'
            AND d.decimals IS NULL THEN sale_amount_unadj
            ELSE sale_amount_unadj / pow(
                10,
                d.decimals
            )
        END AS sale_amount,
        ROUND(
            CASE
                WHEN d.decimals IS NOT NULL
                OR currency_address = 'ETH' THEN creator_fee * price
                ELSE NULL
            END,
            2
        ) AS creator_fee_usd,
        ROUND(
            CASE
                WHEN d.decimals IS NOT NULL
                OR currency_address = 'ETH' THEN platform_fee * price
                ELSE NULL
            END,
            2
        ) AS platform_fee_usd,
        creator_fee + platform_fee AS total_fees,
        ROUND(
            CASE
                WHEN d.decimals IS NOT NULL
                OR currency_address = 'ETH' THEN total_fees * price
                ELSE NULL
            END,
            2
        ) AS total_fees_usd,
        CASE
            WHEN currency_address = 'ETH' THEN creator_fee + sale_amount + platform_fee
            ELSE sale_amount
        END AS adj_price,
        ROUND(
            CASE
                WHEN d.decimals IS NOT NULL
                OR currency_address = 'ETH' THEN adj_price * price
                ELSE NULL
            END,
            2
        ) AS price_usd,
        tx_fee,
        ROUND(
            tx_fee * eth_price,
            2
        ) AS tx_fee_usd
    FROM
        relevant_transfers A
        LEFT JOIN tx_data b
        ON A.tx_hash = b.tx_hash
        LEFT JOIN tx_currency C
        ON A.tx_hash = C.tx_hash
        LEFT JOIN decimals d
        ON C.currency_address = d.address
        LEFT JOIN os_fees_per_nft osf
        ON A.tx_hash = osf.tx_hash
        LEFT JOIN eth_sales_per
        ON A.tx_hash = eth_sales_per.tx_hash
        AND A.from_address = eth_sales_per.to_address
        LEFT JOIN eth_royalties
        ON A.tx_hash = eth_royalties.tx_hash
        LEFT JOIN os_token_fees_per
        ON A.tx_hash = os_token_fees_per.tx_hash
        LEFT JOIN token_royalties
        ON A.tx_hash = token_royalties.tx_hash
        LEFT JOIN tokens_to_seller
        ON A.tx_hash = tokens_to_seller.tx_hash
        LEFT JOIN token_prices
        ON token_prices.hour = DATE_TRUNC(
            'HOUR',
            b.block_timestamp
        )
        AND C.currency_address = token_prices.token_address
        LEFT JOIN eth_prices
        ON eth_prices.hour = DATE_TRUNC(
            'HOUR',
            b.block_timestamp
        )
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
    platform_exchange_version,
    buyer_address,
    seller_address,
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
    ingested_at
FROM
    FINAL
WHERE
    price IS NOT NULL
    AND price_usd < 100000000 qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    ingested_at DESC)) = 1
