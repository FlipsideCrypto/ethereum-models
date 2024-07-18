{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH settings AS (

    SELECT
        '2022-04-15' AS start_date,
        '2022-10-21 04:09:59.000' AS end_date,
        '0x20f780a973856b93f63670377900c1d2a50a77c4' AS main_address,
        '0x00ca62445b06a9adc1879a44485b4efdcb7b75f3' AS fee_address,
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS wrapped_native_address
),
raw AS (
    SELECT
        tx_hash,
        event_index,
        event_name,
        decoded_flat,
        IFF(
            event_name LIKE '%Buy%',
            'bid_won',
            'sale'
        ) AS event_type,
        decoded_flat :erc20Token :: STRING AS currency_address_raw,
        COALESCE(
            decoded_flat :erc20TokenAmount,
            decoded_flat :erc20FillAmount
        ) :: INT AS amount_raw,
        COALESCE(
            decoded_flat :erc721Token,
            decoded_flat :erc1155Token
        ) :: STRING AS nft_address,
        COALESCE(
            decoded_flat :erc721TokenId,
            decoded_flat :erc1155TokenId
        ) :: STRING AS tokenid,
        decoded_flat :erc1155FillAmount :: STRING AS erc1155_value,
        IFF(
            erc1155_value IS NULL,
            'erc721',
            'erc1155'
        ) AS nft_type,
        decoded_flat :maker :: STRING AS maker,
        decoded_flat :taker :: STRING AS taker,
        IFF(
            event_name LIKE '%Buy%',
            taker,
            maker
        ) AS seller_address,
        IFF(
            event_name LIKE '%Buy%',
            maker,
            taker
        ) AS buyer_address,
        decoded_flat :fees AS fees_array,
        decoded_flat :orderHash :: STRING AS orderhash,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS intra_grouping_seller_fill,
        block_timestamp,
        block_number,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_timestamp :: DATE >= (
            SELECT
                start_date
            FROM
                settings
        )
        AND contract_address = (
            SELECT
                main_address
            FROM
                settings
        )
        AND event_name IN (
            'ERC721BuyOrderFilled',
            'ERC721SellOrderFilled',
            'ERC1155SellOrderFilled',
            'ERC1155BuyOrderFilled'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
old_token_transfers AS (
    -- old version only has 1 sale event per tx
    SELECT
        tx_hash,
        event_index,
        from_address,
        to_address,
        contract_address AS currency_address_raw,
        raw_amount,
        CASE
            WHEN ROW_NUMBER() over (
                PARTITION BY tx_hash,
                contract_address
                ORDER BY
                    raw_amount DESC
            ) = 1 THEN raw_amount
            ELSE 0
        END AS net_sale_amount_raw,
        CASE
            WHEN to_address = (
                SELECT
                    fee_address
                FROM
                    settings
            ) THEN raw_amount
            ELSE 0
        END AS platform_amount_raw,
        CASE
            WHEN net_sale_amount_raw = 0
            AND platform_amount_raw = 0 THEN raw_amount
            ELSE 0
        END AS creator_amount_raw
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        block_timestamp :: DATE >= (
            SELECT
                start_date
            FROM
                settings
        )
        AND block_timestamp <= (
            SELECT
                end_date
            FROM
                settings
        )
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                raw
            WHERE
                currency_address_raw != '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
old_token_transfers_agg AS (
    SELECT
        tx_hash,
        currency_address_raw,
        SUM(net_sale_amount_raw) AS net_sale_raw,
        SUM(platform_amount_raw) AS platform_fee_raw,
        SUM(creator_amount_raw) AS creator_fee_raw
    FROM
        old_token_transfers
    GROUP BY
        ALL
),
old_eth_transfers AS (
    SELECT
        tx_hash,
        trace_index,
        from_address,
        to_address,
        eth_value,
        eth_value * pow(
            10,
            18
        ) AS amount_raw,
        IFF(
            to_address = (
                SELECT
                    main_address
                FROM
                    settings
            ),
            1,
            0
        ) AS intra_grouping
    FROM
        {{ ref('silver__traces') }}
    WHERE
        block_timestamp :: DATE >= (
            SELECT
                start_date
            FROM
                settings
        )
        AND block_timestamp <= (
            SELECT
                end_date
            FROM
                settings
        )
        AND (
            from_address = (
                SELECT
                    main_address
                FROM
                    settings
            )
            OR (
                to_address = (
                    SELECT
                        main_address
                    FROM
                        settings
                )
                AND from_address != (
                    SELECT
                        wrapped_native_address
                    FROM
                        settings
                )
            )
        )
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                raw
            WHERE
                currency_address_raw = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
        )
        AND TYPE = 'CALL'
        AND eth_value > 0
        AND tx_status = 'SUCCESS'
        AND trace_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
old_eth_labels AS (
    SELECT
        *,
        SUM(intra_grouping) over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) AS intra_grouping_seller
    FROM
        old_eth_transfers
),
old_eth_amounts AS (
    SELECT
        *,
        CASE
            WHEN ROW_NUMBER() over (
                PARTITION BY tx_hash,
                intra_grouping_seller
                ORDER BY
                    amount_raw DESC
            ) = 1 THEN amount_raw
            ELSE 0
        END AS sale_amount,
        CASE
            WHEN to_address = (
                SELECT
                    fee_address
                FROM
                    settings
            ) THEN amount_raw
            ELSE 0
        END AS platform_fee,
        CASE
            WHEN sale_amount = 0
            AND platform_fee = 0 THEN amount_raw
            ELSE 0
        END AS creator_fee
    FROM
        old_eth_labels
    WHERE
        to_address != (
            SELECT
                main_address
            FROM
                settings
        )
),
old_eth_agg AS (
    SELECT
        tx_hash,
        intra_grouping_seller,
        SUM(sale_amount) AS net_sale_raw,
        SUM(platform_fee) AS platform_fee_raw,
        SUM(creator_fee) AS creator_fee_raw
    FROM
        old_eth_amounts
    GROUP BY
        ALL
),
old_eth_agg_rn AS (
    SELECT
        tx_hash,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                intra_grouping_seller ASC
        ) AS intra_grouping_seller_fill,
        net_sale_raw,
        platform_fee_raw,
        creator_fee_raw
    FROM
        old_eth_agg
),
old_eth_base AS (
    SELECT
        tx_hash,
        intra_grouping_seller_fill,
        event_index,
        event_name,
        decoded_flat,
        event_type,
        currency_address_raw,
        amount_raw,
        nft_address,
        tokenid,
        erc1155_value,
        nft_type,
        maker,
        taker,
        seller_address,
        buyer_address,
        net_sale_raw + platform_fee_raw + creator_fee_raw AS total_price_raw,
        platform_fee_raw + creator_fee_raw AS total_fees_raw,
        platform_fee_raw,
        creator_fee_raw,
        fees_array,
        orderhash,
        block_timestamp,
        block_number,
        _log_id,
        _inserted_timestamp
    FROM
        raw
        INNER JOIN old_eth_agg_rn USING (
            tx_hash,
            intra_grouping_seller_fill
        )
    WHERE
        block_timestamp <= (
            SELECT
                end_date
            FROM
                settings
        )
        AND currency_address_raw = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
),
old_token_base AS (
    SELECT
        tx_hash,
        intra_grouping_seller_fill,
        event_index,
        event_name,
        decoded_flat,
        event_type,
        currency_address_raw,
        amount_raw,
        nft_address,
        tokenid,
        erc1155_value,
        nft_type,
        maker,
        taker,
        seller_address,
        buyer_address,
        net_sale_raw + platform_fee_raw + creator_fee_raw AS total_price_raw,
        platform_fee_raw + creator_fee_raw AS total_fees_raw,
        platform_fee_raw,
        creator_fee_raw,
        fees_array,
        orderhash,
        block_timestamp,
        block_number,
        _log_id,
        _inserted_timestamp
    FROM
        raw
        INNER JOIN old_token_transfers_agg USING (
            tx_hash,
            currency_address_raw
        )
    WHERE
        block_timestamp <= (
            SELECT
                end_date
            FROM
                settings
        )
        AND currency_address_raw != '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
),
raw_fees AS (
    SELECT
        tx_hash,
        event_index,
        INDEX,
        VALUE :amount :: INT AS fee_amount_raw,
        VALUE :recipient :: STRING AS fee_recipient,
        CASE
            WHEN fee_recipient = (
                SELECT
                    fee_address
                FROM
                    settings
            ) THEN fee_amount_raw
            ELSE 0
        END AS platform_amount_raw,
        CASE
            WHEN fee_recipient != (
                SELECT
                    fee_address
                FROM
                    settings
            ) THEN fee_amount_raw
            ELSE 0
        END AS creator_amount_raw
    FROM
        raw,
        LATERAL FLATTEN (
            input => fees_array
        )
    WHERE
        block_timestamp > (
            SELECT
                end_date
            FROM
                settings
        )
),
raw_fees_agg AS (
    SELECT
        tx_hash,
        event_index,
        SUM(platform_amount_raw) AS platform_fee_raw_,
        SUM(creator_amount_raw) AS creator_fee_raw_
    FROM
        raw_fees
    GROUP BY
        ALL
),
new_base AS (
    SELECT
        tx_hash,
        intra_grouping_seller_fill,
        event_index,
        event_name,
        decoded_flat,
        event_type,
        currency_address_raw,
        amount_raw,
        nft_address,
        tokenid,
        erc1155_value,
        nft_type,
        maker,
        taker,
        seller_address,
        buyer_address,
        amount_raw AS total_price_raw,
        COALESCE(
            platform_fee_raw_,
            0
        ) + COALESCE(
            creator_fee_raw_,
            0
        ) AS total_fees_raw,
        COALESCE(
            platform_fee_raw_,
            0
        ) AS platform_fee_raw,
        COALESCE(
            creator_fee_raw_,
            0
        ) AS creator_fee_raw,
        fees_array,
        orderhash,
        block_timestamp,
        block_number,
        _log_id,
        _inserted_timestamp
    FROM
        raw
        LEFT JOIN raw_fees_agg USING (
            tx_hash,
            event_index
        )
    WHERE
        block_timestamp > (
            SELECT
                end_date
            FROM
                settings
        )
),
all_combined AS (
    SELECT
        *
    FROM
        old_eth_base
    UNION ALL
    SELECT
        *
    FROM
        old_token_base
    UNION ALL
    SELECT
        *
    FROM
        new_base
),
tx_data AS (
    SELECT
        tx_hash,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        origin_function_signature,
        tx_fee,
        input_data
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        block_timestamp :: DATE >= (
            SELECT
                start_date
            FROM
                settings
        )
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                all_combined
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    decoded_flat,
    event_type,
    (
        SELECT
            main_address
        FROM
            settings
    ) AS platform_address,
    'element' AS platform_name,
    'element v1' AS platform_exchange_version,
    intra_grouping_seller_fill,
    currency_address_raw,
    amount_raw,
    nft_address,
    tokenid,
    erc1155_value,
    nft_type,
    maker,
    taker,
    seller_address,
    buyer_address,
    IFF(
        currency_address_raw = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
        'ETH',
        currency_address_raw
    ) AS currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    fees_array,
    orderhash,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_fee,
    input_data,
    _log_id,
    CONCAT(
        nft_address,
        '-',
        tokenId,
        '-',
        platform_exchange_version,
        '-',
        _log_id
    ) AS nft_log_id,
    _inserted_timestamp
FROM
    all_combined
    INNER JOIN tx_data USING (tx_hash) qualify ROW_NUMBER() over (
        PARTITION BY nft_log_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
