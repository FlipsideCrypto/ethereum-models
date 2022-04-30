{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH opensea_sales AS (

    SELECT
        _log_id,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        event_name,
        event_inputs,
        event_index,
        event_inputs :maker :: STRING AS maker_address,
        event_inputs :taker :: STRING AS taker_address,
        event_inputs :price :: INTEGER AS unadj_price,
        ingested_at :: TIMESTAMP AS ingested_at,
        ROW_NUMBER() over(
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS agg_id
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address IN (
            '0x7be8076f4ea4a4ad08075c2508e481d6c946d12b',
            '0x7f268357a8c2552623316e2562d90e642bb538e5'
        )
        AND event_name = 'OrdersMatched'

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        )
    FROM
        {{ this }}
)
{% endif %}
),
nft_transfers AS (
    SELECT
        tx_hash,
        contract_address AS nft_address,
        from_address,
        to_address,
        tokenid,
        erc1155_value,
        ingested_at,
        _log_id,
        event_index,
        ROW_NUMBER() over(
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS agg_id
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                opensea_sales
        )

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        )
    FROM
        {{ this }}
)
{% endif %}
),
eth_tx_data AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address,
        to_address,
        eth_value,
        identifier,
        LEFT(identifier, LENGTH(identifier) -2) AS id_group,
        CASE
            WHEN identifier = 'CALL_ORIGIN' THEN 'ORIGIN'
            WHEN from_address IN (
                '0x7be8076f4ea4a4ad08075c2508e481d6c946d12b',
                '0x7f268357a8c2552623316e2562d90e642bb538e5'
            )
            AND to_address = '0x5b3256965e7c3cf26e11fcaf296dfc8807c01073' THEN 'OS_FEE'
            WHEN from_address IN (
                '0x7be8076f4ea4a4ad08075c2508e481d6c946d12b',
                '0x7f268357a8c2552623316e2562d90e642bb538e5'
            )
            AND to_address <> '0x5b3256965e7c3cf26e11fcaf296dfc8807c01073'
            AND identifier <> 'CALL_ORIGIN' THEN 'TO_SELLER'
            WHEN to_address IN (
                '0x7be8076f4ea4a4ad08075c2508e481d6c946d12b',
                '0x7f268357a8c2552623316e2562d90e642bb538e5'
            ) THEN 'SALE_EVENT'
        END AS call_record_type
    FROM
        {{ ref('silver__eth_transfers') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                opensea_sales
        )

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        )
    FROM
        {{ this }}
)
{% endif %}
),
traces_sales_address AS (
    SELECT
        tx_hash,
        from_address,
        to_address,
        eth_value,
        identifier,
        SUBSTR(identifier, 0, LENGTH(identifier) - CHARINDEX('_', REVERSE(identifier))) AS sale_call_id
    FROM
        eth_tx_data
    WHERE
        call_record_type = 'TO_SELLER'),
        traces_sales AS (
            SELECT
                eth_tx_data.tx_hash AS sale_hash,
                eth_tx_data.eth_value AS sale_value,
                eth_tx_data.identifier AS sale_identifer,
                traces_sales_address.to_address AS seller_address,
                SPLIT(
                    sale_identifer,
                    '_'
                ) AS split_id,
                split_id [1] :: INTEGER AS level1,
                split_id [2] :: INTEGER AS level2,
                split_id [3] :: INTEGER AS level3,
                split_id [4] :: INTEGER AS level4,
                split_id [5] :: INTEGER AS level5,
                ROW_NUMBER() over(
                    PARTITION BY sale_hash
                    ORDER BY
                        level1 ASC,
                        level2 ASC,
                        level3 ASC,
                        level4 ASC,
                        level5 ASC
                ) AS agg_id
            FROM
                eth_tx_data
                LEFT JOIN traces_sales_address
                ON eth_tx_data.tx_hash = traces_sales_address.tx_hash
                AND eth_tx_data.identifier = traces_sales_address.sale_call_id
            WHERE
                eth_tx_data.call_record_type = 'SALE_EVENT'
        ),
        os_eth_fees AS (
            SELECT
                tx_hash,
                identifier,
                eth_value AS os_fee,
                SPLIT(
                    identifier,
                    '_'
                ) AS split_id,
                split_id [1] :: INTEGER AS level1,
                split_id [2] :: INTEGER AS level2,
                split_id [3] :: INTEGER AS level3,
                split_id [4] :: INTEGER AS level4,
                split_id [5] :: INTEGER AS level5,
                ROW_NUMBER() over(
                    PARTITION BY tx_hash
                    ORDER BY
                        level1 ASC,
                        level2 ASC,
                        level3 ASC,
                        level4 ASC,
                        level5 ASC
                ) AS agg_id
            FROM
                eth_tx_data
            WHERE
                call_record_type = 'OS_FEE'
        ),
        token_tx_data AS (
            SELECT
                tx_hash,
                contract_address AS currency_address,
                to_address,
                from_address,
                raw_amount,
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
                        opensea_sales
                )

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        )
    FROM
        {{ this }}
)
{% endif %}
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
            WHEN to_address IN (
                '0x7be8076f4ea4a4ad08075c2508e481d6c946d12b',
                '0x7f268357a8c2552623316e2562d90e642bb538e5'
            ) THEN 'DIRECT'
            ELSE 'INDIRECT'
        END AS interaction_type,
        ingested_at
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                opensea_sales
        )

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        )
    FROM
        {{ this }}
)
{% endif %}
),
nfts_per_trade AS (
    SELECT
        tx_hash,
        COUNT(
            DISTINCT tokenid
        ) AS nft_count
    FROM
        nft_transfers
    GROUP BY
        tx_hash
),
eth_sales AS (
    SELECT
        tx_hash,
        'ETH' AS currency_address
    FROM
        opensea_sales
    WHERE
        tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                token_tx_data
        )
),
trade_currency AS (
    SELECT
        tx_hash,
        currency_address
    FROM
        token_tx_data
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
os_token_fees AS (
    SELECT
        tx_hash,
        currency_address,
        to_address,
        from_address,
        raw_amount,
        agg_id
    FROM
        token_tx_data
    WHERE
        to_address = '0x5b3256965e7c3cf26e11fcaf296dfc8807c01073'
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
                opensea_sales
        )
    GROUP BY
        HOUR,
        token_address
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
direct_interactions AS (
    SELECT
        nft_transfers._log_id AS _log_id,
        opensea_sales.block_number AS block_number,
        opensea_sales.block_timestamp AS block_timestamp,
        opensea_sales.tx_hash AS tx_hash,
        contract_address AS platform_address,
        tx_data.tx_fee AS tx_fee,
        tx_fee * eth_price AS tx_fee_usd,
        event_name,
        event_inputs,
        maker_address,
        taker_address,
        nft_transfers.from_address AS nft_from_address,
        nft_transfers.to_address AS nft_to_address,
        nft_address,
        tokenId,
        erc1155_value,
        unadj_price,
        price AS token_price,
        nft_count,
        tx_currency.currency_address AS currency_address,
        CASE
            WHEN tx_currency.currency_address = 'ETH' THEN 'ETH'
            ELSE symbol
        END AS currency_symbol,
        CASE
            WHEN tx_currency.currency_address = 'ETH' THEN 18
            ELSE decimals
        END AS token_decimals,
        opensea_sales.ingested_at AS ingested_at,
        COALESCE(unadj_price / nft_count / pow(10, token_decimals), unadj_price) AS adj_price,
        CASE
            WHEN token_decimals IS NULL THEN NULL
            ELSE ROUND(
                adj_price * token_price,
                2
            )
        END AS price_usd,
        COALESCE(
            os_fee,
            COALESCE(raw_amount / nft_count / pow(10, token_decimals), raw_amount),
            0
        ) AS total_fees,
        CASE
            WHEN total_fees > 0 THEN adj_price * 0.025
            ELSE 0
        END AS platform_fee,
        COALESCE(
            total_fees - platform_fee,
            0
        ) AS creator_fee,
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
        END AS creator_fee_usd
    FROM
        opensea_sales
        INNER JOIN tx_data
        ON opensea_sales.tx_hash = tx_data.tx_hash
        AND tx_data.interaction_type = 'DIRECT'
        LEFT JOIN nft_transfers
        ON opensea_sales.tx_hash = nft_transfers.tx_hash
        AND (
            (
                opensea_sales.maker_address = nft_transfers.from_address
                AND opensea_sales.taker_address = nft_transfers.to_address
            )
            OR (
                opensea_sales.maker_address = nft_transfers.to_address
                AND opensea_sales.taker_address = nft_transfers.from_address
            )
        )
        LEFT JOIN tx_currency
        ON tx_currency.tx_hash = opensea_sales.tx_hash
        LEFT JOIN decimals
        ON tx_currency.currency_address = decimals.address
        LEFT JOIN token_prices
        ON token_prices.hour = DATE_TRUNC(
            'HOUR',
            opensea_sales.block_timestamp
        )
        AND tx_currency.currency_address = token_prices.token_address
        LEFT JOIN nfts_per_trade
        ON nfts_per_trade.tx_hash = opensea_sales.tx_hash
        LEFT JOIN os_eth_fees
        ON os_eth_fees.tx_hash = opensea_sales.tx_hash
        LEFT JOIN os_token_fees
        ON os_token_fees.tx_hash = opensea_sales.tx_hash
        AND os_token_fees.currency_address = decimals.address
        LEFT JOIN eth_prices
        ON eth_prices.hour = DATE_TRUNC(
            'HOUR',
            opensea_sales.block_timestamp
        )
),
indirect_interactions AS (
    SELECT
        nft_transfers._log_id AS _log_id,
        nft_transfers.tx_hash AS tx_hash,
        tx_data.block_timestamp AS block_timestamp,
        tx_data.block_number AS block_number,
        tx_data.ingested_at AS ingested_at,
        nft_transfers.from_address AS nft_from_address,
        nft_transfers.to_address AS nft_to_address,
        nft_transfers.nft_address AS nft_address,
        nft_transfers.tokenId AS tokenId,
        nft_transfers.erc1155_value AS erc1155_value,
        tx_data.tx_fee AS tx_fee,
        tx_fee * eth_price AS tx_fee_usd,
        platform.contract_address AS platform_address,
        tx_currency.currency_address AS currency_address,
        CASE
            WHEN tx_currency.currency_address = 'ETH' THEN 'ETH'
            ELSE symbol
        END AS currency_symbol,
        CASE
            WHEN tx_currency.currency_address = 'ETH' THEN 18
            ELSE decimals
        END AS token_decimals,
        sale_value,
        CASE
            WHEN token_decimals IS NULL THEN NULL
            ELSE ROUND(
                sale_value * price,
                2
            )
        END AS price_usd,
        COALESCE(
            os_fee,
            COALESCE(raw_amount / nft_count / pow(10, token_decimals), raw_amount),
            0
        ) AS total_fees,
        CASE
            WHEN total_fees > 0 THEN sale_value * 0.025
            ELSE 0
        END AS platform_fee,
        COALESCE(
            total_fees - platform_fee,
            0
        ) AS creator_fee,
        CASE
            WHEN token_decimals IS NULL THEN NULL
            ELSE ROUND(
                total_fees * price,
                2
            )
        END AS total_fees_usd,
        CASE
            WHEN token_decimals IS NULL THEN NULL
            ELSE ROUND(
                platform_fee * price,
                2
            )
        END AS platform_fee_usd,
        CASE
            WHEN token_decimals IS NULL THEN NULL
            ELSE ROUND(
                creator_fee * price,
                2
            )
        END AS creator_fee_usd
    FROM
        nft_transfers
        INNER JOIN tx_data
        ON nft_transfers.tx_hash = tx_data.tx_hash
        AND tx_data.interaction_type = 'INDIRECT'
        INNER JOIN traces_sales
        ON traces_sales.sale_hash = nft_transfers.tx_hash
        AND nft_transfers.from_address = traces_sales.seller_address
        AND nft_transfers.agg_id = traces_sales.agg_id
        LEFT JOIN tx_currency
        ON tx_currency.tx_hash = nft_transfers.tx_hash
        LEFT JOIN os_eth_fees
        ON os_eth_fees.tx_hash = nft_transfers.tx_hash
        AND os_eth_fees.agg_id = nft_transfers.agg_id
        LEFT JOIN os_token_fees
        ON os_token_fees.tx_hash = nft_transfers.tx_hash
        AND os_token_fees.agg_id = nft_transfers.agg_id
        LEFT JOIN decimals
        ON tx_currency.currency_address = decimals.address
        LEFT JOIN token_prices
        ON token_prices.hour = DATE_TRUNC(
            'HOUR',
            tx_data.block_timestamp
        )
        AND tx_currency.currency_address = token_prices.token_address
        LEFT JOIN (
            SELECT
                DISTINCT tx_hash,
                contract_address
            FROM
                opensea_sales
        ) AS platform
        ON platform.tx_hash = tx_data.tx_hash
        LEFT JOIN nfts_per_trade
        ON nfts_per_trade.tx_hash = tx_data.tx_hash
        LEFT JOIN eth_prices
        ON eth_prices.hour = DATE_TRUNC(
            'HOUR',
            tx_data.block_timestamp
        )
),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        platform_address,
        'opensea' AS platform_name,
        nft_from_address,
        nft_to_address,
        nft_address,
        erc1155_value,
        tokenId,
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
        direct_interactions
    UNION
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        platform_address,
        'opensea' AS platform_name,
        nft_from_address,
        nft_to_address,
        nft_address,
        erc1155_value,
        tokenId,
        currency_symbol,
        currency_address,
        sale_value AS price,
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
        indirect_interactions
),
nft_metadata AS (
    SELECT
        address AS project_address,
        label
    FROM
        {{ ref('core__dim_labels') }}
    WHERE
        address IN (
            SELECT
                DISTINCT nft_address
            FROM
                FINAL
        )
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    platform_address,
    platform_name,
    nft_from_address,
    nft_to_address,
    nft_address,
    label AS project_name,
    erc1155_value,
    tokenId,
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
    _log_id,
    ingested_at
FROM
    FINAL
    LEFT JOIN nft_metadata
    ON nft_metadata.project_address = FINAL.nft_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    ingested_at DESC)) = 1
