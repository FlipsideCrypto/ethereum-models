{{ config(
    materialized = 'incremental',
    unique_key = 'nft_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH seaport_fees_wallet AS (

    SELECT
        *
    FROM
        (
            VALUES
                ('0x0000a26b00c1f0df003000390027140000faa719'),
                ('0x8de9c5a032463c561423387a9648c5c7bcc5bc90'),
                ('0x5b3256965e7c3cf26e11fcaf296dfc8807c01073')
        ) t (addresses)
),
raw_decoded_logs AS (
    SELECT
        *
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_number >= 16530300
        AND contract_address = '0x00000000000001ad428e4906ae43d8f9852d0dd6'
        AND event_name = 'OrderFulfilled'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE 
    FROM
        {{ this }}
)
{% endif %}
),
mao_buy_tx AS (
    SELECT
        tx_hash,
        event_index,
        CASE
            WHEN decoded_data :data [4] :value [0] [0] IN (
                2,
                3
            ) THEN 'buy'
            WHEN decoded_data :data [4] :value [0] [0] IN (1) THEN 'offer_accepted'
            ELSE NULL
        END AS trade_type
    FROM
        raw_decoded_logs
    WHERE
        trade_type = 'buy'
),
mao_offer_accepted_tx AS (
    SELECT
        block_number,
        tx_hash,
        CASE
            WHEN decoded_data :data [4] :value [0] [0] IN (
                2,
                3
            ) THEN 'buy'
            WHEN decoded_data :data [4] :value [0] [0] IN (1) THEN 'offer_accepted'
            ELSE NULL
        END AS trade_type
    FROM
        raw_decoded_logs
    WHERE
        trade_type = 'offer_accepted'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                mao_buy_tx
        )
),
raw_logs AS (
    SELECT
        *
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_number >= 16530300
        AND contract_address = '0x00000000000001ad428e4906ae43d8f9852d0dd6'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE 
    FROM
        {{ this }}
)
{% endif %}
),
mao_orderhash AS (
    SELECT
        tx_hash,
        CONCAT(
            '0x',
            VALUE :: STRING
        ) AS orderhash,
        CONCAT(
            tx_hash,
            '-',
            orderHash
        ) AS tx_hash_orderhash
    FROM
        raw_logs,
        TABLE(
            FLATTEN(
                input => regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}'))
            )
            WHERE
                topics [0] :: STRING = '0x4b9f2d36e1b4c93de62cc077b00b1a91d84b6c31b4a14e012718dcca230689e7'
                AND INDEX IN (
                    2,
                    3
                )
                AND tx_hash IN (
                    SELECT
                        tx_hash
                    FROM
                        mao_offer_accepted_tx
                )
        ),
        mao_orderhash_full AS (
            SELECT
                tx_hash,
                regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
                CONCAT(
                    '0x',
                    segmented_data [2] :: STRING
                ) AS orderhash_1,
                CONCAT(
                    '0x',
                    segmented_data [3] :: STRING
                ) AS orderhash_2,
                CONCAT(
                    tx_hash,
                    '-',
                    orderhash_1,
                    '-',
                    orderhash_2
                ) AS tx_hash_orderhash_full
            FROM
                raw_logs
            WHERE
                topics [0] :: STRING = '0x4b9f2d36e1b4c93de62cc077b00b1a91d84b6c31b4a14e012718dcca230689e7'
                AND tx_hash IN (
                    SELECT
                        tx_hash
                    FROM
                        mao_offer_accepted_tx
                )
        ),
        mao_raw_decoded AS (
            SELECT
                CASE
                    WHEN decoded_data :data [4] :value [0] [0] IN (
                        2,
                        3
                    ) THEN 'buy'
                    WHEN decoded_data :data [4] :value [0] [0] IN (1) THEN 'offer_accepted'
                    ELSE NULL
                END AS trade_type,
                decoded_flat :orderHash :: STRING AS orderhash,
                CONCAT(
                    tx_hash,
                    '-',
                    decoded_flat :orderHash :: STRING
                ) AS tx_hash_orderhash,*
            FROM
                raw_decoded_logs
            WHERE
                tx_hash IN (
                    SELECT
                        tx_hash
                    FROM
                        mao_orderhash_full
                )
                AND tx_hash_orderhash IN (
                    SELECT
                        tx_hash_orderhash
                    FROM
                        mao_orderhash
                )
        ),
        seaport_tx_table AS (
            SELECT
                block_timestamp,
                tx_hash
            FROM
                raw_logs
            WHERE
                block_timestamp >= '2023-02-01'
                AND topics [0] = '0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31'
        ),
        decoded AS (
            SELECT
                tx_hash,
                decoded_flat,
                event_index,
                decoded_data,
                CONCAT(
                    tx_hash,
                    '-',
                    decoded_flat :orderHash :: STRING
                ) AS tx_hash_orderhash,
                _log_id,
                _inserted_timestamp,
                LOWER(
                    decoded_data :address :: STRING
                ) AS contract_address,
                decoded_data :name :: STRING AS event_name,
                CASE
                    WHEN decoded_data :data [4] :value [0] [0] IN (
                        2,
                        3
                    ) THEN 'buy'
                    WHEN decoded_data :data [4] :value [0] [0] IN (1) THEN 'offer_accepted'
                    ELSE NULL
                END AS trade_type
            FROM
                raw_decoded_logs
            WHERE
                tx_hash IN (
                    SELECT
                        tx_hash
                    FROM
                        seaport_tx_table
                )
                AND tx_hash_orderhash NOT IN (
                    SELECT
                        tx_hash_orderhash
                    FROM
                        mao_orderhash
                )
        ),
        offer_length_match_advanced_order_oa AS (
            SELECT
                tx_hash,
                decoded_flat :orderHash :: STRING AS orderhash,
                COUNT(1) AS offer_length_raw
            FROM
                mao_raw_decoded,
                TABLE(FLATTEN(input => decoded_flat :consideration))
            WHERE
                trade_type = 'offer_accepted'
                AND VALUE :itemType IN (
                    2,
                    3
                )
                AND decoded_flat :consideration [0] IS NOT NULL
            GROUP BY
                tx_hash,
                decoded_flat :orderHash :: STRING
            HAVING
                offer_length_raw IS NOT NULL
        ),
        offer_length_match_advanced_order_buy AS (
            SELECT
                tx_hash,
                decoded_flat :orderHash :: STRING AS orderhash,
                COUNT(1) AS offer_length_raw
            FROM
                mao_raw_decoded,
                TABLE(FLATTEN(input => decoded_flat :consideration))
            WHERE
                trade_type = 'buy'
                AND VALUE :itemType IN (
                    2,
                    3
                )
                AND decoded_flat :consideration [0] IS NOT NULL
            GROUP BY
                tx_hash,
                decoded_flat :orderHash :: STRING
        ),
        offer_length_match_advanced_order_combined AS (
            SELECT
                *
            FROM
                offer_length_match_advanced_order_oa
            UNION ALL
            SELECT
                *
            FROM
                offer_length_match_advanced_order_buy
        ),
        offer_length_match_advanced_order AS (
            SELECT
                C.tx_hash,
                COALESCE(
                    A.tx_hash_orderhash_full,
                    b.tx_hash_orderhash_full
                ) AS tx_hash_orderhash_full,
                offer_length_raw
            FROM
                offer_length_match_advanced_order_combined C
                LEFT JOIN mao_orderhash_full A
                ON C.tx_hash = A.tx_hash
                AND C.orderhash = A.orderhash_1
                LEFT JOIN mao_orderhash_full b
                ON C.tx_hash = b.tx_hash
                AND C.orderhash = b.orderhash_2
        ),
        offer_length_count_buy AS (
            SELECT
                tx_hash,
                event_index,
                this,
                COUNT(
                    VALUE [0]
                ) AS offer_length_raw --> this is the number of nfts in a batch buy. If n = 1, then price is known. If n > 1 then price is estimated
            FROM
                decoded,
                TABLE(FLATTEN(input => decoded_data :data [4] :value))
            WHERE
                trade_type = 'buy'
                AND VALUE [0] IN (
                    2,
                    3
                )
            GROUP BY
                tx_hash,
                event_index,
                this
        ),
        offer_length_count_offer AS (
            SELECT
                tx_hash,
                event_index,
                this,
                COUNT(
                    VALUE [0]
                ) AS offer_length_raw --> this is the number of nfts in a batch buy. If n = 1, then price is known. If n > 1 then price is estimated
            FROM
                decoded,
                TABLE(FLATTEN(input => decoded_data :data [5] :value))
            WHERE
                trade_type = 'offer_accepted'
                AND VALUE [0] IN (
                    2,
                    3
                )
            GROUP BY
                tx_hash,
                event_index,
                this
        ),
        flat_raw AS (
            SELECT
                tx_hash,
                event_index,
                contract_address,
                event_name,
                trade_type,
                decoded_data :data AS full_data,
                _log_id,
                _inserted_timestamp,
                OBJECT_AGG(
                    VALUE :name,
                    VALUE :value
                ) AS decoded_output
            FROM
                decoded,
                LATERAL FLATTEN(
                    input => decoded_data :data
                ) f
            WHERE
                event_name IN (
                    'OrderFulfilled',
                    'OrdersMatched'
                )
                AND trade_type IS NOT NULL
            GROUP BY
                tx_hash,
                event_index,
                contract_address,
                event_name,
                trade_type,
                full_data,
                _log_id,
                _inserted_timestamp
        ),
        flat AS (
            SELECT
                r.tx_hash,
                r.event_index,
                contract_address,
                event_name,
                COALESCE (
                    b.offer_length_raw,
                    o.offer_length_raw,
                    NULL
                ) AS offer_length,
                trade_type,
                full_data,
                _log_id,
                _inserted_timestamp,
                decoded_output
            FROM
                flat_raw r
                LEFT JOIN offer_length_count_buy b
                ON r.tx_hash = b.tx_hash
                AND r.event_index = b.event_index
                LEFT JOIN offer_length_count_offer o
                ON r.tx_hash = o.tx_hash
                AND r.event_index = o.event_index
            WHERE
                offer_length IS NOT NULL
        ),
        filtered_private_offer_tx AS (
            SELECT
                tx_hash
            FROM
                flat
            WHERE
                trade_type = 'offer_accepted'
                AND full_data [3] :value :: STRING = '0x0000000000000000000000000000000000000000'
        ),
        private_offer_tx_flat AS (
            SELECT
                tx_hash,
                ARRAY_AGG (
                    CASE
                        WHEN trade_type = 'buy' THEN full_data [1] :value :: STRING
                    END
                ) [0] :: STRING AS private_offerer,
                ARRAY_AGG(
                    CASE
                        WHEN trade_type = 'offer_accepted' THEN full_data [1] :value :: STRING
                    END
                ) [0] :: STRING AS private_recipient --(buyer) or receiver of the NFT
            FROM
                flat
            WHERE
                tx_hash IN (
                    SELECT
                        tx_hash
                    FROM
                        filtered_private_offer_tx
                )
            GROUP BY
                tx_hash
        ),
        private_offer_tx_flat_null_offerer AS (
            -- mainly transfers via opensea as a result of a phishing scam)
            SELECT
                tx_hash,
                full_data [1] :value :: STRING AS private_seller -- person whose account is compromised
            FROM
                flat
            WHERE
                tx_hash IN (
                    SELECT
                        tx_hash
                    FROM
                        private_offer_tx_flat
                    WHERE
                        private_offerer IS NULL
                )
                AND trade_type = 'offer_accepted'
        ),
        private_offer_tx_offerer_combined AS (
            SELECT
                t.tx_hash,
                t.private_offerer,
                t.private_recipient AS recipient_from_private_tx,
                n.private_seller,
                CASE
                    WHEN t.private_offerer IS NULL THEN n.private_seller
                    ELSE t.private_offerer
                END AS offerer_from_private_tx
            FROM
                private_offer_tx_flat t
                LEFT JOIN private_offer_tx_flat_null_offerer n
                ON t.tx_hash = n.tx_hash
        ),
        base_sales_buy AS (
            SELECT
                tx_hash,
                event_index,
                contract_address,
                event_name,
                offer_length,
                decoded_output :offerer :: STRING AS offerer,
                --seller
                decoded_output :orderHash :: STRING AS orderHash,
                decoded_output :recipient :: STRING AS recipient_temp,
                -- buyer
                CASE
                    WHEN recipient_temp = '0x0000000000000000000000000000000000000000' THEN 'private'
                    ELSE 'public'
                END AS sale_category,
                trade_type,
                IFF(
                    offer_length > 1,
                    'true',
                    'false'
                ) AS is_price_estimated,
                decoded_output :zone :: STRING AS ZONE,
                decoded_output :consideration [0] [0] AS tx_type,
                decoded_output,
                decoded_output :consideration AS consideration,
                decoded_output :offer AS offer,
                _log_id,
                _inserted_timestamp
            FROM
                flat
            WHERE
                event_name IN (
                    'OrderFulfilled'
                )
                AND trade_type = 'buy'
                AND tx_hash NOT IN (
                    SELECT
                        tx_hash
                    FROM
                        filtered_private_offer_tx
                )
                AND tx_type IS NOT NULL
        ),
        base_sales_buy_null_values AS (
            SELECT
                tx_hash,
                event_index,
                contract_address,
                event_name,
                offer_length,
                decoded_output :offerer :: STRING AS offerer,
                decoded_output :orderHash :: STRING AS orderHash,
                decoded_output :recipient :: STRING AS recipient_temp,
                CASE
                    WHEN recipient_temp = '0x0000000000000000000000000000000000000000' THEN 'private'
                    ELSE 'public'
                END AS sale_category,
                trade_type,
                IFF(
                    offer_length > 1,
                    'true',
                    'false'
                ) AS is_price_estimated,
                decoded_output :zone :: STRING AS ZONE,
                decoded_output :consideration [0] [0] AS tx_type,
                decoded_output,
                decoded_output :consideration AS consideration,
                decoded_output :offer AS offer,
                _log_id,
                _inserted_timestamp
            FROM
                flat
            WHERE
                event_name IN (
                    'OrderFulfilled'
                )
                AND trade_type = 'buy'
                AND tx_hash NOT IN (
                    SELECT
                        tx_hash
                    FROM
                        filtered_private_offer_tx
                )
                AND tx_hash NOT IN (
                    SELECT
                        tx_hash
                    FROM
                        base_sales_buy
                )
                AND tx_type IS NULL
        ),
        base_sales_buy_public_nft_transfers AS (
            SELECT
                tx_hash,
                event_index,
                t.index AS flatten_index,
                -- 2 = erc721, 3 = erc1155
                t.value [0] AS token_type,
                t.value [1] :: STRING AS nft_address,
                t.value [2] :: STRING AS tokenid,
                t.value [3] AS number_of_item_or_erc1155 -- if token type = 3 then erc1155_value, else null
            FROM
                flat,
                TABLE(FLATTEN(input => decoded_output :offer)) t
            WHERE
                tx_hash IN (
                    SELECT
                        tx_hash
                    FROM
                        base_sales_buy
                    WHERE
                        sale_category = 'public'
                )
                AND t.value [0] :: STRING IN (
                    2,
                    3
                )
                AND nft_address IS NOT NULL
        ),
        base_sales_buy_public_nft_transfers_null_values AS (
            SELECT
                tx_hash,
                event_index,
                t.index AS flatten_index,
                -- 2 = erc721, 3 = erc1155
                t.value [0] AS token_type,
                t.value [1] :: STRING AS nft_address,
                t.value [2] :: STRING AS tokenid,
                t.value [3] AS number_of_item_or_erc1155 -- if token type = 3 then erc1155_value, else null
            FROM
                flat,
                TABLE(FLATTEN(input => decoded_output :offer)) t
            WHERE
                tx_hash IN (
                    SELECT
                        tx_hash
                    FROM
                        base_sales_buy_null_values
                    WHERE
                        sale_category = 'public'
                )
                AND t.value [0] :: STRING IN (
                    2,
                    3
                )
                AND nft_address IS NOT NULL
        ),
        base_sales_buy_public_nft_transfers_combined AS (
            SELECT
                *
            FROM
                base_sales_buy_public_nft_transfers
            UNION ALL
            SELECT
                *
            FROM
                base_sales_buy_public_nft_transfers_null_values
        ),
        base_sales_buy_private_nft_transfers AS (
            SELECT
                tx_hash,
                event_index,
                t.index AS flatten_index,
                t.value [0] AS token_type,
                t.value [1] :: STRING AS nft_address,
                t.value [2] :: STRING AS tokenid,
                t.value [3] AS number_of_item_or_erc1155,
                t.value [4] :: STRING AS private_sale_recipient
            FROM
                flat,
                TABLE(FLATTEN(input => decoded_output :consideration)) t
            WHERE
                tx_hash IN (
                    SELECT
                        tx_hash
                    FROM
                        base_sales_buy
                    WHERE
                        sale_category = 'private'
                )
                AND t.value [0] :: STRING IN (
                    2,
                    3
                )
                AND nft_address IS NOT NULL
        ),
        base_sales_buy_sale_amount_filter AS (
            SELECT
                tx_hash,
                event_index,
                t.value AS first_flatten_value,
                t.index AS first_flatten_index
            FROM
                flat,
                TABLE(FLATTEN(input => decoded_output :consideration)) t
            WHERE
                tx_hash IN (
                    SELECT
                        tx_hash
                    FROM
                        base_sales_buy
                )
                AND t.value [0] :: STRING IN (
                    0,
                    1
                )
        ),
        base_sales_buy_address_list_flatten AS (
            SELECT
                tx_hash,
                event_index,
                first_flatten_index,
                first_flatten_value [1] :: STRING AS currency_address,
                first_flatten_value [3] :: INT AS raw_amount,
                first_flatten_value [4] :: STRING AS address_list,
                CASE
                    WHEN first_flatten_index = 0 THEN raw_amount
                    ELSE 0
                END AS sale_amount_raw_,
                CASE
                    WHEN first_flatten_index > 0
                    AND address_list IN (
                        SELECT
                            addresses
                        FROM
                            seaport_fees_wallet
                    ) THEN raw_amount
                    ELSE 0
                END AS platform_fee_raw_,
                CASE
                    WHEN first_flatten_index > 0
                    AND address_list NOT IN (
                        SELECT
                            addresses
                        FROM
                            seaport_fees_wallet
                    ) THEN raw_amount
                    ELSE 0
                END AS creator_fee_raw_
            FROM
                base_sales_buy_sale_amount_filter
        ),
        base_sales_buy_address_list_flatten_agg AS (
            SELECT
                tx_hash,
                event_index,
                currency_address,
                SUM(sale_amount_raw_) AS sale_amount_raw_,
                SUM(platform_fee_raw_) AS platform_fee_raw_,
                SUM(creator_fee_raw_) AS creator_fee_raw_
            FROM
                base_sales_buy_address_list_flatten
            GROUP BY
                tx_hash,
                event_index,
                currency_address
        ),
        base_sales_buy_sale_amount_null_values AS (
            SELECT
                tx_hash,
                event_index,
                NULL AS currency_address,
                0 AS sale_amount_raw_,
                0 AS platform_fee_raw_,
                0 AS creator_fee_raw_
            FROM
                base_sales_buy_null_values
            UNION ALL
            SELECT
                tx_hash,
                event_index,
                NULL AS currency_address,
                0 AS sale_amount_raw_,
                0 AS platform_fee_raw_,
                0 AS creator_fee_raw_
            FROM
                base_sales_buy
            WHERE
                tx_hash NOT IN (
                    SELECT
                        tx_hash
                    FROM
                        base_sales_buy_sale_amount_filter
                )
        ),
        base_sales_buy_sale_amount_combined AS (
            SELECT
                *
            FROM
                base_sales_buy_address_list_flatten_agg
            UNION ALL
            SELECT
                *
            FROM
                base_sales_buy_sale_amount_null_values
        ),
        base_sales_buy_combined AS (
            SELECT
                *
            FROM
                base_sales_buy
            UNION ALL
            SELECT
                *
            FROM
                base_sales_buy_null_values
        ),
        base_sales_buy_final_public AS (
            SELECT
                b.tx_hash,
                b.event_index,
                b.contract_address,
                b.event_name,
                b.offer_length,
                b.offerer,
                b.orderHash,
                recipient_temp AS recipient,
                sale_category,
                b.trade_type,
                is_price_estimated,
                ZONE,
                tx_type,
                n.token_type,
                n.nft_address,
                n.tokenid,
                CASE
                    WHEN n.token_type = '3' THEN n.number_of_item_or_erc1155
                    ELSE NULL
                END AS erc1155_value,
                currency_address,
                sale_amount_raw_ / b.offer_length AS sale_amount_raw,
                platform_fee_raw_ / b.offer_length AS platform_fee_raw,
                creator_fee_raw_ / b.offer_length AS creator_fee_raw,
                creator_fee_raw + platform_fee_raw AS total_fees_raw,
                total_fees_raw + sale_amount_raw AS total_sale_amount_raw,
                decoded_output,
                consideration,
                offer,
                _log_id,
                _inserted_timestamp
            FROM
                base_sales_buy_combined b
                INNER JOIN base_sales_buy_sale_amount_combined s
                ON b.tx_hash = s.tx_hash
                AND b.event_index = s.event_index full
                OUTER JOIN base_sales_buy_public_nft_transfers_combined n
                ON b.tx_hash = n.tx_hash
                AND b.event_index = n.event_index
            WHERE
                nft_address IS NOT NULL
        ),
        base_sales_buy_final_private AS (
            SELECT
                b.tx_hash,
                b.event_index,
                b.contract_address,
                b.event_name,
                b.offer_length,
                b.offerer,
                b.orderHash,
                private_sale_recipient AS recipient,
                sale_category,
                b.trade_type,
                is_price_estimated,
                ZONE,
                tx_type,
                n.token_type,
                n.nft_address,
                n.tokenid,
                CASE
                    WHEN n.token_type = '3' THEN n.number_of_item_or_erc1155
                    ELSE NULL
                END AS erc1155_value,
                currency_address,
                sale_amount_raw_ / b.offer_length AS sale_amount_raw,
                platform_fee_raw_ / b.offer_length AS platform_fee_raw,
                creator_fee_raw_ / b.offer_length AS creator_fee_raw,
                creator_fee_raw + platform_fee_raw AS total_fees_raw,
                total_fees_raw + sale_amount_raw AS total_sale_amount_raw,
                decoded_output,
                consideration,
                offer,
                _log_id,
                _inserted_timestamp
            FROM
                base_sales_buy_combined b
                INNER JOIN base_sales_buy_sale_amount_combined s
                ON b.tx_hash = s.tx_hash
                AND b.event_index = s.event_index full
                OUTER JOIN base_sales_buy_private_nft_transfers n
                ON b.tx_hash = n.tx_hash
                AND b.event_index = n.event_index
            WHERE
                nft_address IS NOT NULL
        ),
        base_sales_buy_final AS (
            SELECT
                *
            FROM
                base_sales_buy_final_public
            UNION ALL
            SELECT
                *
            FROM
                base_sales_buy_final_private
        ),
        base_sales_offer_accepted AS (
            SELECT
                tx_hash,
                event_index,
                contract_address,
                event_name,
                offer_length,
                decoded_output :recipient :: STRING AS offerer_temp,
                decoded_output :orderHash :: STRING AS orderHash,
                decoded_output :offerer :: STRING AS recipient_temp,
                CASE
                    WHEN offerer_temp = '0x0000000000000000000000000000000000000000' THEN 'private'
                    ELSE 'public'
                END AS sale_category,
                trade_type,
                IFF(
                    offer_length > 1,
                    'true',
                    'false'
                ) AS is_price_estimated,
                decoded_output :zone :: STRING AS ZONE,
                decoded_output :consideration [0] [0] AS tx_type,
                decoded_output :offer [0] [3] :: INT / offer_length AS total_sale_amount_raw,
                decoded_output,
                decoded_output :consideration AS consideration,
                decoded_output :offer AS offer,
                _log_id,
                _inserted_timestamp
            FROM
                flat
            WHERE
                event_name IN (
                    'OrderFulfilled',
                    'OrdersMatched'
                )
                AND trade_type = 'offer_accepted'
                AND tx_type IS NOT NULL
                AND tx_hash IS NOT NULL
        ),
        base_sales_offer_accepted_nft_transfers AS (
            SELECT
                tx_hash,
                event_index,
                t.index AS flatten_index,
                t.value [0] AS token_type,
                t.value [1] :: STRING AS nft_address,
                t.value [2] :: STRING AS tokenid,
                t.value [3] AS number_of_item_or_erc1155,
                t.value [4] AS private_recipient_from_consideration
            FROM
                flat,
                TABLE(FLATTEN(input => decoded_output :consideration)) t
            WHERE
                tx_hash IN (
                    SELECT
                        tx_hash
                    FROM
                        base_sales_offer_accepted
                )
                AND t.value [0] :: STRING IN (
                    2,
                    3
                )
        ),
        base_sales_offer_accepted_sale_amount_filter AS (
            SELECT
                tx_hash,
                event_index,
                t.value AS first_flatten_value,
                t.index AS first_flatten_index
            FROM
                flat,
                TABLE(FLATTEN(input => decoded_output :consideration)) t
            WHERE
                tx_hash IN (
                    SELECT
                        tx_hash
                    FROM
                        base_sales_offer_accepted
                )
                AND tx_hash NOT IN (
                    SELECT
                        tx_hash
                    FROM
                        private_offer_tx_flat_null_offerer
                )
                AND t.value [0] :: STRING IN (
                    0,
                    1
                )
                AND trade_type = 'offer_accepted'
        ),
        base_sales_offer_accepted_address_list_flatten AS (
            SELECT
                tx_hash,
                event_index,
                first_flatten_index,
                first_flatten_value,
                first_flatten_value [1] :: STRING AS currency_address,
                first_flatten_value [3] :: INT AS raw_amount,
                first_flatten_value [4] :: STRING AS address_list,
                CASE
                    WHEN address_list IN (
                        SELECT
                            addresses
                        FROM
                            seaport_fees_wallet
                    ) THEN raw_amount
                    ELSE 0
                END AS platform_fee_raw_,
                CASE
                    WHEN address_list NOT IN (
                        SELECT
                            addresses
                        FROM
                            seaport_fees_wallet
                    ) THEN raw_amount
                    ELSE 0
                END AS creator_fee_raw_
            FROM
                base_sales_offer_accepted_sale_amount_filter
        ),
        base_sales_offer_accepted_address_list_flatten_agg AS (
            SELECT
                tx_hash,
                event_index,
                NULL AS sale_values,
                currency_address,
                SUM(platform_fee_raw_) AS platform_fee_raw_,
                SUM(creator_fee_raw_) AS creator_fee_raw_
            FROM
                base_sales_offer_accepted_address_list_flatten
            GROUP BY
                tx_hash,
                event_index,
                currency_address
        ),
        base_sales_offer_accepted_no_fees_tx AS (
            SELECT
                tx_hash
            FROM
                base_sales_offer_accepted
            WHERE
                tx_hash NOT IN (
                    SELECT
                        tx_hash
                    FROM
                        base_sales_offer_accepted_sale_amount_filter
                )
                AND tx_hash NOT IN (
                    SELECT
                        tx_hash
                    FROM
                        private_offer_tx_flat_null_offerer
                )
        ),
        base_sales_offer_accepted_no_fees_amount AS (
            SELECT
                tx_hash,
                event_index,
                ARRAY_AGG(
                    t.value
                ) AS sale_values,
                sale_values [0] [1] :: STRING AS currency_address,
                0 AS platform_fee_raw_,
                0 AS creator_fee_raw_
            FROM
                flat,
                TABLE(FLATTEN(input => decoded_output :offer)) t
            WHERE
                tx_hash IN (
                    SELECT
                        tx_hash
                    FROM
                        base_sales_offer_accepted_no_fees_tx
                )
                AND t.value [0] :: STRING IN (
                    0,
                    1
                )
                AND trade_type = 'offer_accepted'
            GROUP BY
                tx_hash,
                event_index
        ),
        base_sales_offer_accepted_phishing_scam_amount AS (
            SELECT
                tx_hash,
                event_index,
                NULL AS sale_values,
                NULL AS currency_address,
                0 AS platform_fee_raw_,
                0 AS creator_fee_raw_
            FROM
                base_sales_offer_accepted_nft_transfers
            WHERE
                tx_hash IN (
                    SELECT
                        tx_hash
                    FROM
                        private_offer_tx_flat_null_offerer
                )
        ),
        base_sales_offer_accepted_sale_and_no_fees AS (
            SELECT
                *
            FROM
                base_sales_offer_accepted_address_list_flatten_agg
            UNION ALL
            SELECT
                *
            FROM
                base_sales_offer_accepted_no_fees_amount
            UNION ALL
            SELECT
                *
            FROM
                base_sales_offer_accepted_phishing_scam_amount
        ),
        base_sales_offer_accepted_final AS (
            SELECT
                b.tx_hash,
                b.event_index,
                contract_address,
                event_name,
                offer_length,
                CASE
                    WHEN sale_category = 'public' THEN offerer_temp
                    ELSE COALESCE (
                        private_offerer,
                        offerer_from_private_tx
                    )
                END AS offerer,
                orderHash,
                CASE
                    WHEN sale_category = 'public' THEN recipient_temp
                    WHEN sale_category = 'private'
                    AND private_offerer IS NULL THEN private_recipient_from_consideration
                    ELSE recipient_from_private_tx
                END AS recipient,
                sale_category,
                b.trade_type,
                is_price_estimated,
                ZONE,
                tx_type,
                token_type,
                nft_address,
                tokenid,
                CASE
                    WHEN token_type = '3' THEN number_of_item_or_erc1155
                    ELSE NULL
                END AS erc1155_value,
                currency_address,
                CASE
                    WHEN private_offerer IS NULL
                    AND private_seller IS NOT NULL THEN 0
                    ELSE decoded_output :offer [0] [3] :: INT / offer_length
                END AS total_sale_amount_raw,
                platform_fee_raw_ :: INT / offer_length AS platform_fee_raw,
                creator_fee_raw_ :: INT / offer_length AS creator_fee_raw,
                platform_fee_raw + creator_fee_raw AS total_fees_raw,
                decoded_output,
                decoded_output :consideration AS consideration,
                decoded_output :offer AS offer,
                _log_id,
                _inserted_timestamp
            FROM
                base_sales_offer_accepted b
                INNER JOIN base_sales_offer_accepted_sale_and_no_fees s
                ON b.tx_hash = s.tx_hash
                AND b.event_index = s.event_index full
                OUTER JOIN base_sales_offer_accepted_nft_transfers n
                ON b.tx_hash = n.tx_hash
                AND b.event_index = n.event_index
                LEFT OUTER JOIN private_offer_tx_offerer_combined p
                ON b.tx_hash = p.tx_hash
            WHERE
                nft_address IS NOT NULL qualify ROW_NUMBER() over (
                    PARTITION BY b.tx_hash,
                    b.event_index,
                    nft_address,
                    tokenid,
                    _log_id
                    ORDER BY
                        _inserted_timestamp ASC
                ) = 1
        ),
        match_advanced_orders_sale_amount AS (
            SELECT
                r.tx_hash,
                block_number,
                r.event_index,
                event_name,
                contract_address,
                trade_type,
                offer_length_raw,
                decoded_data,
                decoded_flat,
                decoded_flat :offerer :: STRING AS offerer,
                -- or the one who proposed the sale so that they receive the nft
                decoded_flat :zone :: STRING AS ZONE,
                decoded_flat :recipient :: STRING AS recipient,
                decoded_flat :orderHash :: STRING AS orderhash,
                COALESCE(
                    A.tx_hash_orderhash_full,
                    b.tx_hash_orderhash_full
                ) AS tx_hash_orderhash_full,
                decoded_flat :offer [0] :token :: STRING AS payment_token,
                (
                    decoded_flat :offer [0] :amount :: INT
                ) / offer_length_raw AS total_sale_price_raw,
                _inserted_timestamp,
                _log_id
            FROM
                mao_raw_decoded r
                LEFT JOIN mao_orderhash_full A
                ON r.tx_hash = A.tx_hash
                AND r.decoded_flat :orderHash :: STRING = A.orderhash_1
                LEFT JOIN mao_orderhash_full b
                ON r.tx_hash = b.tx_hash
                AND r.decoded_flat :orderHash :: STRING = b.orderhash_2
                INNER JOIN offer_length_match_advanced_order o
                ON r.tx_hash = o.tx_hash
                AND COALESCE(
                    A.tx_hash_orderhash_full,
                    b.tx_hash_orderhash_full
                ) = o.tx_hash_orderhash_full
            WHERE
                trade_type = 'offer_accepted'
                AND decoded_flat :offer [0] :itemType :: INT IN (
                    0,
                    1
                )
        ),
        match_advanced_orders_nft_received_oa AS (
            SELECT
                tx_hash,
                event_index,
                decoded_flat :orderHash :: STRING AS orderhash,
                decoded_flat :offerer :: STRING AS transfers_nft_receiver,
                decoded_flat :recipient :: STRING AS transfers_nft_seller,
                VALUE :token :: STRING AS nft_address,
                VALUE :identifier :: STRING AS tokenId,
                VALUE :amount :: INT AS nft_tokenid_quantity,
                VALUE :recipient :: STRING AS nft_recipient,
                VALUE :itemType :: INT AS token_type
            FROM
                mao_raw_decoded,
                LATERAL FLATTEN (
                    input => decoded_flat :consideration
                )
            WHERE
                trade_type = 'offer_accepted'
                AND VALUE :itemType :: INT IN (
                    2,
                    3
                )
        ),
        match_advanced_orders_nft_received_oa_hash AS (
            SELECT
                r.tx_hash,
                COALESCE(
                    A.tx_hash_orderhash_full,
                    b.tx_hash_orderhash_full
                ) AS tx_hash_orderhash_full,
                transfers_nft_receiver,
                transfers_nft_seller,
                nft_address,
                tokenId,
                nft_tokenid_quantity,
                nft_recipient,
                token_type
            FROM
                match_advanced_orders_nft_received_oa r
                LEFT JOIN mao_orderhash_full A
                ON r.tx_hash = A.tx_hash
                AND r.orderHash = A.orderhash_1
                LEFT JOIN mao_orderhash_full b
                ON r.tx_hash = b.tx_hash
                AND r.orderHash = b.orderhash_2
        ),
        match_advanced_orders_nft_received_buy AS (
            SELECT
                tx_hash,
                event_index,
                decoded_flat :orderHash :: STRING AS orderhash,
                decoded_flat :offerer :: STRING AS transfers_nft_seller,
                decoded_flat :recipient :: STRING AS transfers_nft_receiver,
                VALUE :token :: STRING AS nft_address,
                VALUE :identifier :: STRING AS tokenId,
                VALUE :amount :: INT AS nft_tokenid_quantity,
                VALUE :recipient :: STRING AS nft_recipient,
                VALUE :itemType :: INT AS token_type
            FROM
                mao_raw_decoded,
                LATERAL FLATTEN (
                    input => decoded_flat :consideration
                )
            WHERE
                trade_type = 'buy'
                AND VALUE :itemType :: INT IN (
                    2,
                    3
                )
        ),
        match_advanced_orders_nft_received_buy_hash AS (
            SELECT
                r.tx_hash,
                COALESCE(
                    A.tx_hash_orderhash_full,
                    b.tx_hash_orderhash_full
                ) AS tx_hash_orderhash_full,
                transfers_nft_receiver,
                transfers_nft_seller,
                nft_address,
                tokenId,
                nft_tokenid_quantity,
                nft_recipient,
                token_type
            FROM
                match_advanced_orders_nft_received_buy r
                LEFT JOIN mao_orderhash_full A
                ON r.tx_hash = A.tx_hash
                AND r.orderHash = A.orderhash_1
                LEFT JOIN mao_orderhash_full b
                ON r.tx_hash = b.tx_hash
                AND r.orderHash = b.orderhash_2
        ),
        match_advanced_orders_nft_received AS (
            SELECT
                *
            FROM
                match_advanced_orders_nft_received_oa_hash
            UNION ALL
            SELECT
                *
            FROM
                match_advanced_orders_nft_received_buy_hash
        ),
        match_advanced_orders_fees_oa_consideration AS (
            SELECT
                tx_hash,
                decoded_flat,
                event_index,
                VALUE :token :: STRING AS payment_token,
                VALUE :amount :: INT AS raw_amount,
                VALUE :recipient :: STRING AS fee_recipient,
                CASE
                    WHEN fee_recipient IN (
                        SELECT
                            addresses
                        FROM
                            seaport_fees_wallet
                    ) THEN raw_amount
                    ELSE 0
                END AS platform_fee_raw_,
                CASE
                    WHEN fee_recipient NOT IN (
                        SELECT
                            addresses
                        FROM
                            seaport_fees_wallet
                    ) THEN raw_amount
                    ELSE 0
                END AS creator_fee_raw_
            FROM
                mao_raw_decoded,
                LATERAL FLATTEN (
                    input => decoded_flat :consideration
                ) f
            WHERE
                trade_type = 'offer_accepted'
                AND VALUE :itemType :: INT IN (
                    0,
                    1
                )
        ),
        match_advanced_orders_fees_oa_consideration_agg AS (
            SELECT
                tx_hash,
                event_index,
                SUM(platform_fee_raw_) AS platform_fee_raw,
                SUM(creator_fee_raw_) AS creator_fee_raw
            FROM
                match_advanced_orders_fees_oa_consideration
            GROUP BY
                tx_hash,
                event_index
        ),
        match_advanced_orders_fees_buy_consideration AS (
            SELECT
                tx_hash,
                decoded_flat,
                decoded_flat :orderHash :: STRING AS orderhash,
                VALUE :token :: STRING AS payment_token,
                VALUE :amount :: INT AS raw_amount,
                VALUE :recipient :: STRING AS royalty_recipient,
                INDEX,
                CASE
                    WHEN royalty_recipient IN (
                        SELECT
                            addresses
                        FROM
                            seaport_fees_wallet
                    ) THEN raw_amount
                    ELSE 0
                END AS platform_fee_raw_,
                CASE
                    WHEN royalty_recipient NOT IN (
                        SELECT
                            addresses
                        FROM
                            seaport_fees_wallet
                    ) THEN raw_amount
                    ELSE 0
                END AS creator_fee_raw_
            FROM
                mao_raw_decoded,
                LATERAL FLATTEN (
                    input => decoded_flat :consideration
                )
            WHERE
                trade_type = 'buy'
                AND VALUE :itemType :: INT IN (
                    0,
                    1
                ) qualify ROW_NUMBER() over (
                    PARTITION BY tx_hash,
                    orderhash
                    ORDER BY
                        raw_amount DESC
                ) > 1
        ),
        match_advanced_orders_fees_buy_consideration_filtered AS (
            SELECT
                C.tx_hash,
                decoded_flat,
                C.orderhash,
                COALESCE(
                    A.tx_hash_orderhash_full,
                    b.tx_hash_orderhash_full
                ) AS tx_hash_orderhash_full,
                payment_token,
                raw_amount,
                royalty_recipient,
                INDEX,
                platform_fee_raw_,
                creator_fee_raw_
            FROM
                match_advanced_orders_fees_buy_consideration C
                LEFT JOIN mao_orderhash_full A
                ON C.tx_hash = A.tx_hash
                AND C.orderhash = A.orderhash_1
                LEFT JOIN mao_orderhash_full b
                ON C.tx_hash = b.tx_hash
                AND C.orderhash = b.orderhash_2
        ),
        match_advanced_orders_fees_buy_consideration_agg AS (
            SELECT
                tx_hash,
                tx_hash_orderhash_full,
                SUM(platform_fee_raw_) AS platform_fee_raw,
                SUM(creator_fee_raw_) AS creator_fee_raw
            FROM
                match_advanced_orders_fees_buy_consideration_filtered
            GROUP BY
                tx_hash,
                tx_hash_orderhash_full
        ),
        match_advanced_orders_base AS (
            SELECT
                s.tx_hash,
                block_number,
                s.event_index,
                event_name,
                contract_address,
                trade_type,
                offer_length_raw AS offer_length,
                CASE
                    WHEN offerer = '0x0000000000000000000000000000000000000000' THEN 'private'
                    ELSE 'public'
                END AS sale_category,
                IFF(
                    offer_length > 1,
                    'true',
                    'false'
                ) AS is_price_estimated,
                decoded_flat,
                -- using decoded flat here instead of decoded_output so that we're able to filter for match advanced orders
                decoded_flat :consideration AS consideration,
                decoded_flat :offer AS offer,
                decoded_flat :consideration [0] :itemType AS tx_type,
                ZONE,
                transfers_nft_receiver AS recipient,
                --buyer_address in final
                transfers_nft_seller AS offerer,
                --seller_address in final,
                orderhash,
                s.tx_hash_orderhash_full,
                nft_address,
                tokenId,
                token_type,
                CASE
                    WHEN token_type = '3' THEN nft_tokenid_quantity
                    ELSE NULL
                END AS erc1155_value,
                payment_token AS currency_address,
                total_sale_price_raw AS total_sale_amount_raw,
                (COALESCE (fo.platform_fee_raw, 0) + COALESCE(fb.platform_fee_raw, 0)) / offer_length AS platform_fee_raw_total,
                (COALESCE (fo.creator_fee_raw, 0) + COALESCE (fb.creator_fee_raw, 0)) / offer_length AS creator_fee_raw_total,
                platform_fee_raw_total + creator_fee_raw_total AS total_fees_raw,
                _inserted_timestamp,
                _log_id
            FROM
                match_advanced_orders_sale_amount s full
                OUTER JOIN match_advanced_orders_nft_received n
                ON s.tx_hash = n.tx_hash
                AND s.tx_hash_orderhash_full = n.tx_hash_orderhash_full
                LEFT JOIN match_advanced_orders_fees_oa_consideration_agg fo
                ON s.tx_hash = fo.tx_hash
                AND s.event_index = fo.event_index
                LEFT JOIN match_advanced_orders_fees_buy_consideration_agg fb
                ON s.tx_hash = fb.tx_hash
                AND s.tx_hash_orderhash_full = fb.tx_hash_orderhash_full
        ),
        base_sales_buy_and_offer AS (
            SELECT
                tx_hash,
                event_index,
                contract_address,
                event_name,
                offer_length,
                offerer,
                orderHash,
                recipient,
                sale_category,
                trade_type,
                is_price_estimated,
                ZONE,
                tx_type,
                token_type,
                nft_address,
                tokenid AS tokenId,
                erc1155_value,
                IFF(
                    currency_address = '0x0000000000000000000000000000000000000000',
                    'ETH',
                    currency_address
                ) AS currency_address,
                total_sale_amount_raw,
                total_fees_raw,
                platform_fee_raw,
                creator_fee_raw,
                decoded_output,
                consideration,
                offer,
                _log_id,
                _inserted_timestamp
            FROM
                base_sales_buy_final
            UNION ALL
            SELECT
                tx_hash,
                event_index,
                contract_address,
                event_name,
                offer_length,
                offerer,
                orderHash,
                recipient,
                sale_category,
                trade_type,
                is_price_estimated,
                ZONE,
                tx_type,
                token_type,
                nft_address,
                tokenid AS tokenId,
                erc1155_value,
                IFF(
                    currency_address = '0x0000000000000000000000000000000000000000',
                    'ETH',
                    currency_address
                ) AS currency_address,
                total_sale_amount_raw,
                total_fees_raw,
                platform_fee_raw,
                creator_fee_raw,
                decoded_output,
                consideration,
                offer,
                _log_id,
                _inserted_timestamp
            FROM
                base_sales_offer_accepted_final
            UNION ALL
            SELECT
                tx_hash,
                event_index,
                contract_address,
                event_name,
                offer_length,
                offerer,
                orderHash,
                recipient,
                sale_category,
                trade_type,
                is_price_estimated,
                ZONE,
                tx_type,
                token_type,
                nft_address,
                tokenId,
                erc1155_value,
                IFF(
                    currency_address = '0x0000000000000000000000000000000000000000',
                    'ETH',
                    currency_address
                ) AS currency_address,
                total_sale_amount_raw,
                total_fees_raw,
                platform_fee_raw_total AS platform_fee_raw,
                creator_fee_raw_total AS creator_fee_raw,
                decoded_flat,
                consideration,
                offer,
                _log_id,
                _inserted_timestamp
            FROM
                match_advanced_orders_base
        ),
       
        tx_data AS (
            SELECT
                tx_hash,
                block_timestamp,
                block_number,
                from_address,
                to_address,
                origin_function_signature,
                tx_fee,
                input_data
            FROM
                {{ ref('silver__transactions') }}
            WHERE
                block_timestamp :: DATE >= '2023-02-01'
                AND tx_hash IN (
                    SELECT
                        DISTINCT tx_hash
                    FROM
                        base_sales_buy_and_offer
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
        block_timestamp :: DATE >= '2023-02-01'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                base_sales_buy_and_offer
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
final_seaport AS (
    SELECT
        t.block_number,
        t.block_timestamp,
        s.tx_hash,
        s.event_index,
        s.contract_address AS platform_address,
        'opensea' AS platform_name,
        'seaport_1_4' AS platform_exchange_version,
        s.event_name,
        offer_length,
        offerer AS seller_address,
        orderHash,
        recipient AS buyer_address,
        sale_category,
        trade_type,
        CASE
            WHEN trade_type = 'buy' THEN 'sale'
            WHEN trade_type = 'offer_accepted' THEN 'bid_won'
        END AS event_type,
        is_price_estimated,
        ZONE,
        tx_type,
        s.token_type,
        s.nft_address,
        s.tokenId,
        s.erc1155_value,
        s.currency_address,
        total_sale_amount_raw as total_price_raw, 
        total_fees_raw,
        platform_fee_raw,
        creator_fee_raw,
        
        t.tx_fee,
        t.from_address AS origin_from_address,
        t.to_address AS origin_to_address,
        t.origin_function_signature,
        decoded_output,
        consideration,
        offer,
        input_data,
        CONCAT(
            s.nft_address,
            '-',
            s.tokenId,
            '-',
            platform_exchange_version,
            '-',
            _log_id
        ) AS nft_log_id,
        _log_id,
        _inserted_timestamp
    FROM
        base_sales_buy_and_offer s
        INNER JOIN tx_data t
        ON t.tx_hash = s.tx_hash
    
        LEFT JOIN nft_transfers n
        ON n.tx_hash = s.tx_hash
        AND n.contract_address = s.nft_address
        AND n.tokenId = s.tokenId
       
        qualify(ROW_NUMBER() over(PARTITION BY nft_log_id
    ORDER BY
        _inserted_timestamp DESC)) = 1
)
SELECT
    *
FROM
    final_seaport
