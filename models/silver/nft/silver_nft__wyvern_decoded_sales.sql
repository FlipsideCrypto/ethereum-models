{{ config(
    materialized = 'incremental',
    unique_key = 'nft_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH opensea_sales AS (

    SELECT
        block_number,
        tx_hash,
        contract_address,
        event_name,
        event_index,
        decoded_flat :maker :: STRING AS maker_address,
        decoded_flat :taker :: STRING AS taker_address,
        decoded_flat :price :: INTEGER AS unadj_price,
        _log_id,
        TO_TIMESTAMP_NTZ(_inserted_timestamp) AS _inserted_timestamp,
        ROW_NUMBER() over(
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS agg_id
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_number >= 5774644
        AND contract_address IN (
            '0x7be8076f4ea4a4ad08075c2508e481d6c946d12b',
            '0x7f268357a8c2552623316e2562d90e642bb538e5'
        )
        AND event_name = 'OrdersMatched'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
nft_transfers AS (
    SELECT
        block_timestamp,
        tx_hash,
        contract_address AS nft_address,
        from_address,
        to_address,
        tokenid,
        erc1155_value,
        TO_TIMESTAMP_NTZ(_inserted_timestamp) AS _inserted_timestamp,
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
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
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
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
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
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
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
        VALUE AS eth_value,
        tx_fee,
        origin_function_signature,
        CASE
            WHEN to_address IN (
                '0x7be8076f4ea4a4ad08075c2508e481d6c946d12b',
                '0x7f268357a8c2552623316e2562d90e642bb538e5'
            ) THEN 'DIRECT'
            ELSE 'INDIRECT'
        END AS interaction_type,
        TO_TIMESTAMP_NTZ(_inserted_timestamp) AS _inserted_timestamp,
        input_data
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
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
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
direct_interactions AS (
    SELECT
        nft_transfers._log_id AS _log_id,
        opensea_sales.block_number AS block_number,
        tx_data.block_timestamp AS block_timestamp,
        opensea_sales.tx_hash AS tx_hash,
        contract_address AS platform_address,
        tx_data.tx_fee AS tx_fee,
        CASE
            WHEN opensea_sales.maker_address = nft_transfers.from_address THEN 'sale'
            WHEN opensea_sales.maker_address = nft_transfers.to_address THEN 'bid_won'
        END AS event_type,
        event_name,
        maker_address,
        taker_address,
        nft_transfers.from_address AS nft_from_address,
        nft_transfers.to_address AS nft_to_address,
        nft_transfers.event_index AS event_index,
        nft_address,
        tokenId,
        erc1155_value,
        unadj_price,
        nft_count,
        tx_currency.currency_address AS currency_address,
        opensea_sales._inserted_timestamp AS _inserted_timestamp,
        COALESCE(
            unadj_price / nft_count,
            unadj_price
        ) AS adj_price,
        COALESCE(
            os_fee / nft_count,
            COALESCE(
                raw_amount / nft_count,
                raw_amount
            ),
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
        tx_data.to_address AS origin_to_address,
        tx_data.from_address AS origin_from_address,
        tx_data.origin_function_signature AS origin_function_signature,
        tx_data.input_data
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
        LEFT JOIN nfts_per_trade
        ON nfts_per_trade.tx_hash = opensea_sales.tx_hash
        LEFT JOIN os_eth_fees
        ON os_eth_fees.tx_hash = opensea_sales.tx_hash
        LEFT JOIN os_token_fees
        ON os_token_fees.tx_hash = opensea_sales.tx_hash
        AND os_token_fees.currency_address = decimals.address
),
indirect_interactions AS (
    SELECT
        nft_transfers._log_id AS _log_id,
        nft_transfers.tx_hash AS tx_hash,
        tx_data.block_timestamp AS block_timestamp,
        tx_data.block_number AS block_number,
        tx_data._inserted_timestamp AS _inserted_timestamp,
        nft_transfers.from_address AS nft_from_address,
        nft_transfers.to_address AS nft_to_address,
        nft_transfers.nft_address AS nft_address,
        nft_transfers.tokenId AS tokenId,
        nft_transfers.erc1155_value AS erc1155_value,
        nft_transfers.event_index AS event_index,
        tx_data.tx_fee AS tx_fee,
        'sale' AS event_type,
        platform.contract_address AS platform_address,
        tx_currency.currency_address AS currency_address,
        sale_value,
        COALESCE(
            os_fee,
            COALESCE(
                raw_amount / nft_count,
                raw_amount
            ),
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
        tx_data.to_address AS origin_to_address,
        tx_data.from_address AS origin_from_address,
        tx_data.origin_function_signature AS origin_function_signature,
        tx_data.input_data
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
),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_to_address,
        origin_from_address,
        origin_function_signature,
        event_index,
        event_type,
        platform_address,
        'opensea' AS platform_name,
        nft_from_address,
        nft_to_address,
        nft_address,
        erc1155_value,
        tokenId,
        currency_address,
        adj_price AS price,
        total_fees,
        platform_fee,
        creator_fee,
        tx_fee,
        _log_id,
        _inserted_timestamp,
        input_data
    FROM
        direct_interactions
    WHERE
        _log_id IS NOT NULL
    UNION
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_to_address,
        origin_from_address,
        origin_function_signature,
        event_index,
        event_type,
        platform_address,
        'opensea' AS platform_name,
        nft_from_address,
        nft_to_address,
        nft_address,
        erc1155_value,
        tokenId,
        currency_address,
        sale_value AS price,
        total_fees,
        platform_fee,
        creator_fee,
        tx_fee,
        _log_id,
        _inserted_timestamp,
        input_data
    FROM
        indirect_interactions
    WHERE
        _log_id IS NOT NULL
)
SELECT
    block_number,
    block_timestamp,
    origin_to_address,
    origin_from_address,
    origin_function_signature,
    event_index,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    CASE
        WHEN platform_address = LOWER('0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b') THEN 'wyvern_v1'
        WHEN platform_address = LOWER('0x7f268357A8c2552623316e2562D90e642bB538E5') THEN 'wyvern_v2'
    END AS platform_exchange_version,
    nft_from_address AS seller_address,
    nft_to_address AS buyer_address,
    nft_address,
    erc1155_value,
    tokenId,
    currency_address,
    price AS total_price_raw,
    total_fees AS total_fees_raw,
    platform_fee AS platform_fee_raw,
    creator_fee AS creator_fee_raw,
    tx_fee,
    _log_id,
    _inserted_timestamp,
    input_data,
    CONCAT(
        nft_address,
        '-',
        tokenId,
        '-',
        platform_exchange_version,
        '-',
        _log_id
    ) AS nft_log_id
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY nft_log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
