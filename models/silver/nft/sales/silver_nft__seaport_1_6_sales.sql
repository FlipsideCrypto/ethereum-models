{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','nft','curated']
) }}
/*

================================================================================
INITIAL SETUP & CONFIGURATION
================================================================================
Defines fee wallet addresses used to identify platform vs creator fees

*/
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
/*

================================================================================
RAW DATA EXTRACTION
================================================================================
Extracts and prepares raw event logs from Seaport 1.6 contract
Handles both OrderFulfilled and OrdersMatched events

*/
raw_decoded_logs AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        topics,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        event_removed,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_succeeded,
        event_name,
        full_decoded_log,
        full_decoded_log AS decoded_data,
        decoded_log,
        decoded_log AS decoded_flat,
        contract_name,
        ez_decoded_event_logs_id,
        inserted_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp,
        decoded_flat :orderHash :: STRING AS orderhash,
        tx_hash || '-' || decoded_flat :orderHash AS tx_hash_orderhash
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2024-03-15'
        AND block_number >= 19442152
        AND contract_address = '0x0000000000000068f116a894984e2db1123eb395'
        AND event_name IN (
            'OrderFulfilled',
            'OrdersMatched'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
/*

-----
RAW LOGS EXTRACTION
-----
Extracts raw event logs (not decoded) to identify OrdersMatched events
and extract order hashes from the event data

*/
raw_logs AS (
    SELECT
        *,
        IFF(
            topics [0] :: STRING IN (
                '0x4b9f2d36e1b4c93de62cc077b00b1a91d84b6c31b4a14e012718dcca230689e7'
            ),
            1,
            NULL
        ) AS om_event_index_raw,
        IFF(
            topics [0] :: STRING NOT IN (
                '0x4b9f2d36e1b4c93de62cc077b00b1a91d84b6c31b4a14e012718dcca230689e7'
            ),
            SUBSTRING(
                DATA,
                1,
                66
            ),
            NULL
        ) AS order_hash
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2024-03-15'
        AND block_number >= 19442152
        AND contract_address = '0x0000000000000068f116a894984e2db1123eb395'
        AND topics [0] :: STRING IN (
            '0x4b9f2d36e1b4c93de62cc077b00b1a91d84b6c31b4a14e012718dcca230689e7',
            -- ordersMatched
            '0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31' -- orderFulfilled
        )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
/*

-----
EVENT INDEX FILL
-----
Creates a running index for OrdersMatched events within each transaction
This helps group related OrderFulfilled events that belong to the same OrdersMatched
This is because 1 OrdersMatched event can have multiple OrderFulfilled events associated with it.
*/
raw_logs_event_index_fill AS (
    SELECT
        *,
        IFF(
            om_event_index_raw IS NULL,
            NULL,
            SUM(om_event_index_raw) over (
                PARTITION BY tx_hash
                ORDER BY
                    event_index ASC
            )
        ) AS om_event_index
    FROM
        raw_logs
),
/*

-----
ORDERSMATCHED ORDERHASH EXTRACTION
-----
Extracts order hashes from OrdersMatched events by parsing the event data
This helps identify which OrderFulfilled events belong to matchOrders transactions
(mao = matchOrders)

*/
mao_orderhash AS (
    -- this helps us determine which orderhashes belong to OrderMatched as opposed to OrderFulfilled
    SELECT
        tx_hash,
        event_index,
        om_event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented,
        utils.udf_hex_to_int(
            segmented [0] :: STRING
        ) / 32 AS length_index,
        utils.udf_hex_to_int(
            segmented [length_index] :: STRING
        ) AS orders_length,
        INDEX,
        '0x' || VALUE :: STRING AS orderhash,
        tx_hash || '-' || orderhash AS tx_hash_orderhash
    FROM
        raw_logs_event_index_fill,
        LATERAL FLATTEN(
            input => segmented
        )
    WHERE
        INDEX BETWEEN (
            length_index + 1
        )
        AND (
            orders_length + 1
        )
        AND topics [0] :: STRING = '0x4b9f2d36e1b4c93de62cc077b00b1a91d84b6c31b4a14e012718dcca230689e7'
),
/*

================================================================================
ORDERFULFILLED EVENT PROCESSING
================================================================================
Processes OrderFulfilled events that are NOT part of matchOrders transactions
These are standard single-order transactions (fulfillOrder, fulfillAdvancedOrder, etc.)

*/
-- this big section below is for OrderFulfilled
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
        event_name = 'OrderFulfilled'
        AND tx_hash_orderhash NOT IN (
            SELECT
                tx_hash_orderhash
            FROM
                mao_orderhash
        )
),
/*

-----
OFFER LENGTH CALCULATION
-----
Counts the number of NFTs in each order to determine if price is per-item or batch
If offer_length > 1, price is estimated (divided across multiple NFTs)

*/
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
/*

-----
DATA FLATTENING
-----
Flattens the decoded event data into a structured format
Creates decoded_output object with all event parameters

*/
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
            'OrderFulfilled'
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
/*

-----
PRIVATE OFFER HANDLING
-----
Identifies and processes private offers where recipient is zero address
Handles edge cases like phishing scams where offerer may be null

*/
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
/*

================================================================================
BUY TRADE PROCESSING (OrderFulfilled - Buy)
================================================================================
Processes standard buy transactions where seller lists NFT and buyer purchases
Handles both public and private sales, extracts NFT transfers and payment amounts

*/
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
        ) -- AND tx_type IS NOT NULL ; null tx type would mean that there aren't any considerations. The nft is transferred without asking for anything in exchange for it
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
/*

-----
NFT TRANSFERS FOR BUY TRADES
-----
Extracts NFT transfer information from the offer array
Handles both ERC721 and ERC1155 tokens, public and private sales

*/
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
/*

-----
SALE AMOUNT & FEE CALCULATION FOR BUY TRADES
-----
Extracts payment amounts from consideration array
Separates sale amount, platform fees, and creator fees based on recipient addresses

*/
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
        first_flatten_value,
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
        tx_type IS NULL
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
/*

-----
FINAL BUY SALES ASSEMBLY
-----
Combines NFT transfers with sale amounts and fees
Divides amounts by offer_length for batch purchases
Separates public and private sales

*/
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
/*

================================================================================
OFFER ACCEPTED TRADE PROCESSING (OrderFulfilled - Offer Accepted)
================================================================================
Processes transactions where a buyer's offer is accepted by a seller
Handles both public and private offers, including edge cases like phishing scams

*/
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
/*

================================================================================
ORDERSMATCHED (matchOrders) EVENT PROCESSING
================================================================================
Processes complex matchOrders transactions where multiple orders are matched
This is more complex than OrderFulfilled because:
- Multiple orders can be matched in one transaction
- Orders can be deals (NFT-for-NFT swaps) or standard sales
- OrderFulfilled events must be grouped by their OrdersMatched event

*/
-- the new matchOrders logic starts here
/*

-----
ORDERHASH GROUPING
-----
Groups OrderFulfilled events by their associated OrdersMatched event
Creates a grouping key (om_event_index_fill) to link related orders

*/
orderhash_grouping_raw AS (
    SELECT
        tx_hash,
        event_index,
        order_hash,
        om_event_index,
        -- shows the order of multiple ordersmatched event in one tx
        om_event_index_raw,
        CASE
            WHEN om_event_index IS NULL THEN LEAD(om_event_index) ignore nulls over (
                PARTITION BY tx_hash
                ORDER BY
                    event_index ASC
            )
            ELSE NULL
        END AS om_event_index_fill -- shows all orderhashes and their grouping
    FROM
        raw_logs_event_index_fill
),
orderhash_grouping_fill AS (
    SELECT
        tx_hash,
        event_index,
        order_hash,
        CONCAT(
            tx_hash,
            '-',
            order_hash
        ) AS tx_hash_orderhash,
        om_event_index,
        om_event_index_fill,
        om_event_index_raw,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            om_event_index_fill
            ORDER BY
                event_index ASC
        ) AS orderhash_order_within_group -- orderhash order within an orderhash group
    FROM
        orderhash_grouping_raw
),
-- item type references:
-- 0 - eth
-- 1 - erc20
-- 2 - erc721
-- 3 - erc1155
-- 4  - erc721 w criteria,
-- 5 - erc1155 w criteria
/*

-----
CONSIDERATION PROCESSING
-----
Flattens and processes the consideration array from OrderFulfilled events
Identifies NFT transfers and payment amounts
Creates NFT address-identifier pairs to link payments to specific NFTs
Handles forward-filling of NFT identifiers for payment items

*/
mao_consideration_all AS (
    SELECT
        tx_hash,
        event_index,
        decoded_flat,
        INDEX,
        orderhash,
        tx_hash_orderhash,
        decoded_flat :offerer :: STRING AS offerer,
        decoded_flat :recipient :: STRING AS recipient,
        VALUE,
        VALUE :amount :: INT AS amount,
        VALUE :identifier :: STRING AS identifier,
        VALUE :itemType :: INT AS item_type,
        VALUE :recipient :: STRING AS item_recipient,
        VALUE :token :: STRING AS item_token_address,
        decoded_flat :offer [0] :amount :: INT AS offer_amount,
        decoded_flat :offer [0] :identifier :: STRING AS offer_identifier,
        decoded_flat :offer [0] :itemType :: INT AS offer_item_type,
        decoded_flat :offer [0] :token :: STRING AS offer_item_token_address,
        IFF(item_type NOT IN (0, 1), CONCAT(item_token_address, '-', identifier, '-', amount), CONCAT(offer_item_token_address, '-', offer_identifier, '-', offer_amount)) AS nft_address_identifier,
        IFF(item_type NOT IN (0, 1), nft_address_identifier, NULL) AS nft_address_identifier_null,
        CASE
            WHEN item_type NOT IN (
                0,
                1
            ) THEN nft_address_identifier
            WHEN item_type IN (
                0,
                1
            )
            AND offer_item_type IN (
                0,
                1
            ) THEN LAG(nft_address_identifier_null) ignore nulls over (
                PARTITION BY tx_hash,
                event_index
                ORDER BY
                    INDEX ASC
            )
            WHEN item_type IN (
                0,
                1
            )
            AND offer_item_type NOT IN (
                0,
                1
            ) THEN nft_address_identifier
            ELSE nft_address_identifier
        END AS nft_address_identifier_fill_raw,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index
            ORDER BY
                INDEX ASC
        ) AS orderhash_within_event_index_rn,
        _log_id,
        _inserted_timestamp
    FROM
        raw_decoded_logs,
        LATERAL FLATTEN (
            input => decoded_flat :consideration
        )
    WHERE
        event_name = 'OrderFulfilled'
        AND tx_hash_orderhash IN (
            SELECT
                tx_hash_orderhash
            FROM
                mao_orderhash
        )
),
mao_consideration_all_joined AS (
    SELECT
        *,
        CASE
            WHEN nft_address_identifier_null IS NULL
            AND offerer = recipient THEN LAG(nft_address_identifier_null) ignore nulls over (
                PARTITION BY tx_hash,
                om_event_index_fill
                ORDER BY
                    event_index,
                    INDEX ASC
            )
            ELSE nft_address_identifier_fill_raw
        END AS nft_address_identifier_fill,
        IFF(item_type NOT IN (0, 1)
        AND offer_item_type NOT IN (0, 1)
        AND decoded_flat :consideration [0] :itemType :: INT NOT IN (0, 1), CONCAT(tx_hash_orderhash, '-', event_index), NULL) AS deals_tag,
        CONCAT(
            tx_hash_orderhash,
            '-',
            event_index
        ) AS event_index_tag -- tagging for deals tag
    FROM
        mao_consideration_all
        INNER JOIN orderhash_grouping_fill USING (
            tx_hash,
            tx_hash_orderhash,
            event_index
        )
),
/*

-----
SALE AMOUNT CALCULATION FOR NON-DEALS
-----
For standard sales (not deals), extracts payment amounts from consideration
Identifies sale amount (to seller), platform fees, and creator fees
Links payments to specific NFTs using nft_address_identifier_fill

*/
mao_consideration_sale_amounts AS (
    SELECT
        tx_hash,
        orderhash,
        tx_hash_orderhash,
        orderhash_within_event_index_rn,
        om_event_index_fill,
        deals_tag,
        event_index_tag,
        event_index,
        INDEX,
        amount,
        identifier,
        item_type,
        item_token_address,
        item_token_address AS currency_address,
        item_recipient,
        nft_address_identifier_fill,
        CASE
            WHEN amount > 0
            AND ROW_NUMBER() over (
                PARTITION BY tx_hash,
                om_event_index_fill,
                nft_address_identifier_fill
                ORDER BY
                    amount DESC
            ) = 1 THEN amount
            ELSE 0
        END AS sale_amount_raw_,
        CASE
            WHEN item_recipient IN (
                SELECT
                    addresses
                FROM
                    seaport_fees_wallet
            ) THEN amount
            ELSE 0
        END AS platform_fee_raw_,
        CASE
            WHEN sale_amount_raw_ = 0
            AND platform_fee_raw_ = 0 THEN amount
            ELSE 0
        END AS creator_fee_raw_,
        IFF(
            sale_amount_raw_ > 0,
            item_recipient,
            NULL
        ) AS sale_receiver_address
    FROM
        mao_consideration_all_joined
    WHERE
        item_type IN (
            0,
            1
        )
        AND nft_address_identifier_fill IS NOT NULL
        AND event_index_tag NOT IN (
            SELECT
                deals_tag
            FROM
                mao_consideration_all_joined
            WHERE
                deals_tag IS NOT NULL
        ) -- exclude deals
),
mao_nondeals_sale_amount AS (
    SELECT
        tx_hash,
        om_event_index_fill,
        nft_address_identifier_fill,
        currency_address,
        SUM(sale_amount_raw_) AS sale_amount_raw,
        SUM(platform_fee_raw_) AS platform_fee_raw,
        SUM(creator_fee_raw_) AS creator_fee_raw
    FROM
        mao_consideration_sale_amounts
    GROUP BY
        ALL
),
mao_nondeals_sale_amount_receiver AS (
    SELECT
        tx_hash,
        om_event_index_fill,
        nft_address_identifier_fill,
        sale_receiver_address,
        currency_address,
        sale_amount_raw,
        platform_fee_raw,
        creator_fee_raw
    FROM
        mao_nondeals_sale_amount
        INNER JOIN mao_consideration_sale_amounts s USING (
            tx_hash,
            om_event_index_fill,
            nft_address_identifier_fill,
            currency_address
        )
    WHERE
        s.sale_receiver_address IS NOT NULL
),
/*

-----
DEALS PROCESSING (NFT-for-NFT Swaps)
-----
Handles deals where NFTs are exchanged for other NFTs (or NFTs + payment)
More complex because both sides involve NFTs, not just payment
Determines which party is the "buyer" and "seller" based on payment flow

*/
mao_deals_sale_amount AS (
    SELECT
        *,
        IFF(
            item_recipient = offerer,
            recipient,
            offerer
        ) AS payment_from_address,
        item_recipient AS payment_to_address,
        CASE
            WHEN ROW_NUMBER() over (
                PARTITION BY tx_hash,
                om_event_index_fill
                ORDER BY
                    amount DESC
            ) = 1 THEN amount
            ELSE 0
        END AS sale_amount_raw_,
        CASE
            WHEN item_recipient IN (
                SELECT
                    addresses
                FROM
                    seaport_fees_wallet
            ) THEN amount
            ELSE 0
        END AS platform_fee_raw_,
        CASE
            WHEN sale_amount_raw_ = 0
            AND platform_fee_raw_ = 0 THEN amount
            ELSE 0
        END AS creator_fee_raw_
    FROM
        mao_consideration_all_joined
    WHERE
        item_type IN (
            0,
            1
        )
        AND event_index_tag IN (
            SELECT
                deals_tag
            FROM
                mao_consideration_all_joined
            WHERE
                deals_tag IS NOT NULL
        ) -- include deals only
),
mao_deals_sale_amount_agg AS (
    SELECT
        tx_hash,
        om_event_index_fill,
        payment_from_address,
        payment_to_address,
        1 AS deals_sale_rn,
        item_token_address AS currency_address,
        SUM(sale_amount_raw_) AS sale_amount_raw,
        SUM(platform_fee_raw_) AS platform_fee_raw,
        SUM(creator_fee_raw_) AS creator_fee_raw
    FROM
        mao_deals_sale_amount
    GROUP BY
        ALL
),
/*

-----
NFT TRANSFERS FOR MATCHORDERS
-----
Extracts NFT transfer information from consideration array
Separates deals (NFT-for-NFT) from non-deals (standard sales)
Determines buyer and seller addresses for each NFT transfer

*/
mao_deals_nft_transfers AS (
    SELECT
        *,
        IFF(
            item_recipient = offerer,
            recipient,
            offerer
        ) AS nft_from_address,
        item_recipient AS nft_to_address,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            om_event_index_fill
            ORDER BY
                INDEX ASC
        ) AS deals_nft_transfers_rn
    FROM
        mao_consideration_all_joined
    WHERE
        deals_tag IS NOT NULL
),
mao_deals_nft_transfers_sales AS (
    SELECT
        t.tx_hash,
        t.om_event_index_fill,
        orderhash,
        tx_hash_orderhash,
        event_index,
        INDEX,
        orderhash_within_event_index_rn,
        deals_tag,
        event_index_tag,
        nft_address_identifier_fill,
        item_token_address AS nft_address,
        identifier AS tokenid,
        item_type,
        IFF(item_type IN (2, 4), 'erc721', 'erc1155') AS nft_standard,
        IFF(
            nft_standard = 'erc1155',
            amount,
            NULL
        ) AS erc1155_value,
        nft_from_address AS seller_address,
        nft_to_address AS buyer_address,
        COALESCE(
            sale_amount_raw,
            0
        ) AS sale_amount_raw,
        COALESCE(
            platform_fee_raw,
            0
        ) AS platform_fee_raw,
        COALESCE(
            creator_fee_raw,
            0
        ) AS creator_fee_raw,
        currency_address,
        decoded_flat,
        offerer,
        recipient,
        _log_id,
        _inserted_timestamp
    FROM
        mao_deals_nft_transfers t
        LEFT JOIN mao_deals_sale_amount_agg s
        ON t.tx_hash = s.tx_hash
        AND t.om_event_index_fill = s.om_event_index_fill
        AND s.payment_from_address = t.nft_to_address
        AND s.payment_to_address = t.nft_from_address
        AND s.deals_sale_rn = t.deals_nft_transfers_rn
),
mao_nondeals_nft_transfers_sales AS (
    SELECT
        tx_hash,
        om_event_index_fill,
        -- the event index for the ordermatched
        orderhash,
        tx_hash_orderhash,
        -- tx hash and orderhash
        event_index,
        -- event index for orderfulfilled
        INDEX,
        -- index for flattened for consideration
        orderhash_within_event_index_rn,
        -- index of orderhash within a single event index
        deals_tag,
        event_index_tag,
        -- gets the tx hash and event index to tag seaport deals
        A.nft_address_identifier_fill,
        item_token_address AS nft_address,
        identifier AS tokenid,
        item_type,
        IFF(item_type IN (2, 4), 'erc721', 'erc1155') AS nft_standard,
        IFF(
            nft_standard = 'erc1155',
            amount,
            NULL
        ) AS erc1155_value,
        item_recipient AS buyer_address,
        sale_receiver_address,
        IFF(
            item_recipient = offerer,
            recipient,
            offerer
        ) AS seller_address_temp,
        IFF(
            seller_address_temp = buyer_address,
            sale_receiver_address,
            seller_address_temp
        ) AS seller_address,
        offerer,
        recipient,
        decoded_flat,
        currency_address,
        sale_amount_raw,
        platform_fee_raw,
        creator_fee_raw,
        _log_id,
        _inserted_timestamp
    FROM
        mao_consideration_all_joined A
        LEFT JOIN mao_nondeals_sale_amount_receiver USING (
            tx_hash,
            om_event_index_fill,
            nft_address_identifier_fill
        )
    WHERE
        deals_tag IS NULL
        AND item_type NOT IN (
            0,
            1
        )
),
/*

-----
FILL MISSING VALUES FOR NON-DEALS
-----
Forward-fills sale amounts and fees for NFT transfers that don't have direct payment items
Uses LAG/LEAD to propagate values from adjacent consideration items

*/
mao_nondeals_nft_transfers_sales_fill AS (
    SELECT
        *,
        CASE
            WHEN sale_amount_raw IS NULL THEN COALESCE(LAG(sale_amount_raw) ignore nulls over (PARTITION BY tx_hash, om_event_index_fill
            ORDER BY
                INDEX ASC), LEAD(sale_amount_raw) ignore nulls over (PARTITION BY tx_hash, om_event_index_fill
            ORDER BY
                INDEX ASC), 0)
                ELSE sale_amount_raw
        END AS sale_amount_raw_fill,
        CASE
            WHEN platform_fee_raw IS NULL THEN COALESCE(LAG(platform_fee_raw) ignore nulls over (PARTITION BY tx_hash, om_event_index_fill
            ORDER BY
                INDEX ASC), LEAD(platform_fee_raw) ignore nulls over (PARTITION BY tx_hash, om_event_index_fill
            ORDER BY
                INDEX ASC), 0)
                ELSE platform_fee_raw
        END AS platform_fee_raw_fill,
        CASE
            WHEN creator_fee_raw IS NULL THEN COALESCE(LAG(creator_fee_raw) ignore nulls over (PARTITION BY tx_hash, om_event_index_fill
            ORDER BY
                INDEX ASC), LEAD(creator_fee_raw) ignore nulls over (PARTITION BY tx_hash, om_event_index_fill
            ORDER BY
                INDEX ASC), 0)
                ELSE creator_fee_raw
        END AS creator_fee_raw_fill,
        CASE
            WHEN currency_address IS NULL THEN COALESCE(LAG(currency_address) ignore nulls over (PARTITION BY tx_hash, om_event_index_fill
            ORDER BY
                INDEX ASC), LEAD(currency_address) ignore nulls over (PARTITION BY tx_hash, om_event_index_fill
            ORDER BY
                INDEX ASC), NULL)
                ELSE currency_address
        END AS currency_address_fill
    FROM
        mao_nondeals_nft_transfers_sales
),
/*

-----
OFFER LENGTH CALCULATION FOR MATCHORDERS
-----
Counts NFTs in each matchOrders group to determine if prices should be divided
Separates NFTs with known prices from those without (for accurate counting)

*/
mao_nondeals_offer_length_one AS (
    SELECT
        tx_hash,
        om_event_index_fill,
        event_index,
        COUNT(1) AS offer_length_one -- counting how many nfts are in this sale batch , need to add with the following subquery
    FROM
        mao_nondeals_nft_transfers_sales
    WHERE
        sale_amount_raw IS NULL
    GROUP BY
        ALL
),
mao_nondeals_offer_length_two AS (
    -- measuring offer length for non batched
    SELECT
        tx_hash,
        om_event_index_fill,
        event_index,
        COUNT(1) AS offer_length_two -- counting how many nfts are in this sale batch , need to add with the following subquery
    FROM
        mao_nondeals_nft_transfers_sales
    WHERE
        sale_amount_raw IS NOT NULL
    GROUP BY
        ALL
),
mao_nondeals_offer_length AS (
    -- all sales are not grouped by event index, but grouped by the orderMatched index
    SELECT
        tx_hash,
        om_event_index_fill,
        event_index,
        offer_length_two + COALESCE(
            offer_length_one,
            0
        ) AS offer_length --offer length for all
    FROM
        mao_nondeals_offer_length_two
        LEFT JOIN mao_nondeals_offer_length_one USING (
            tx_hash,
            om_event_index_fill,
            event_index
        )
),
mao_nondeals_nft_transfers_sales_fill_offer_length AS (
    SELECT
        tx_hash,
        om_event_index_fill,
        -- the event index for the ordermatched
        orderhash,
        tx_hash_orderhash,
        -- tx hash and orderhash
        event_index,
        -- event index for orderfulfilled
        INDEX,
        -- index for flattened for consideration
        orderhash_within_event_index_rn,
        -- index of orderhash within a single event index
        deals_tag,
        event_index_tag,
        -- gets the tx hash and event index to tag seaport deals
        nft_address_identifier_fill,
        nft_address,
        item_type,
        tokenid,
        nft_standard,
        erc1155_value,
        buyer_address,
        seller_address,
        offerer,
        recipient,
        decoded_flat,
        currency_address_fill AS currency_address,
        offer_length,
        sale_amount_raw_fill / offer_length AS sale_amount_raw,
        platform_fee_raw_fill / offer_length AS platform_fee_raw,
        creator_fee_raw_fill / offer_length AS creator_fee_raw,
        _log_id,
        _inserted_timestamp
    FROM
        mao_nondeals_nft_transfers_sales_fill
        INNER JOIN mao_nondeals_offer_length USING (
            tx_hash,
            om_event_index_fill,
            event_index
        )
),
/*

-----
COMBINED MATCHORDERS BASE
-----
Combines deals and non-deals into a unified structure
Categorizes as 'sale' or 'bid_won' based on consideration item types
Deals always have offer_length = 1 (no division needed)

*/
mao_combined_base AS (
    -- includes both deals and nondeals
    SELECT
        tx_hash,
        om_event_index_fill,
        IFF(
            decoded_flat :consideration [0] :itemType :: INT IN (
                0,
                1
            ),
            'sale',
            'bid_won'
        ) AS event_type,
        IFF(
            event_type = 'sale',
            'mao_deals_buy',
            'mao_deals_oa'
        ) AS category,
        orderhash,
        tx_hash_orderhash,
        event_index,
        INDEX,
        orderhash_within_event_index_rn,
        deals_tag,
        event_index_tag,
        nft_address_identifier_fill,
        nft_address,
        item_type,
        tokenid,
        nft_standard,
        erc1155_value,
        seller_address,
        buyer_address,
        currency_address,
        1 AS offer_length,
        -- deals with always have 1 since we'll not be dividing the sale amount with number of nfts exchanged since nfts are also used as part of the offer
        sale_amount_raw,
        platform_fee_raw,
        creator_fee_raw,
        platform_fee_raw + creator_fee_raw AS total_fees_raw,
        sale_amount_raw + total_fees_raw AS total_price_raw,
        offerer,
        recipient,
        decoded_flat,
        _log_id,
        _inserted_timestamp
    FROM
        mao_deals_nft_transfers_sales
    UNION ALL
    SELECT
        tx_hash,
        om_event_index_fill,
        -- the event index for the ordermatched
        IFF(
            decoded_flat :consideration [0] :itemType :: INT IN (
                0,
                1
            ),
            'sale',
            'bid_won'
        ) AS event_type,
        IFF(
            event_type = 'sale',
            'mao_nondeals_buy',
            'mao_nondeals_oa'
        ) AS category,
        orderhash,
        tx_hash_orderhash,
        event_index,
        INDEX,
        orderhash_within_event_index_rn,
        deals_tag,
        event_index_tag,
        nft_address_identifier_fill,
        nft_address,
        item_type,
        tokenid,
        nft_standard,
        erc1155_value,
        seller_address,
        buyer_address,
        currency_address,
        offer_length,
        sale_amount_raw,
        platform_fee_raw,
        creator_fee_raw,
        platform_fee_raw + creator_fee_raw AS total_fees_raw,
        sale_amount_raw + total_fees_raw AS total_price_raw,
        offerer,
        recipient,
        decoded_flat,
        _log_id,
        _inserted_timestamp
    FROM
        mao_nondeals_nft_transfers_sales_fill_offer_length
),
/*

================================================================================
FINAL ASSEMBLY & ENRICHMENT
================================================================================
Combines all processed sales (OrderFulfilled buy, OrderFulfilled offer accepted,
and OrdersMatched) into a unified structure
Enriches with transaction data and NFT transfer information

*/
base_sales_buy_and_offer AS (
    -- this part combines OrderFulfilled subqueries with OrdersMatched (mao)
    SELECT
        tx_hash,
        'fulfil_buy' AS category,
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
        nft_address AS nft_address_temp,
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
        NULL AS seller_address_temp,
        NULL AS buyer_address_temp,
        _log_id,
        _inserted_timestamp
    FROM
        base_sales_buy_final
    UNION ALL
    SELECT
        tx_hash,
        'fulfil_oa' AS category,
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
        nft_address AS nft_address_temp,
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
        NULL AS seller_address_temp,
        NULL AS buyer_address_temp,
        _log_id,
        _inserted_timestamp
    FROM
        base_sales_offer_accepted_final
    UNION ALL
    SELECT
        tx_hash,
        category,
        event_index,
        '0x0000000000000068f116a894984e2db1123eb395' AS contract_address,
        'OrderFulfilled' AS event_name,
        offer_length,
        offerer,
        orderHash,
        recipient,
        CASE
            WHEN recipient = '0x0000000000000000000000000000000000000000' THEN 'private'
            ELSE 'public'
        END AS sale_category,
        IFF(
            event_type = 'sale',
            'buy',
            'offer_accepted'
        ) AS trade_type,
        IFF(
            offer_length > 1,
            'true',
            'false'
        ) AS is_price_estimated,
        decoded_flat :zone :: STRING AS ZONE,
        item_type AS tx_type,
        item_type AS token_type,
        nft_address AS nft_address_temp,
        tokenId,
        erc1155_value,
        IFF(
            currency_address = '0x0000000000000000000000000000000000000000',
            'ETH',
            currency_address
        ) AS currency_address,
        total_price_raw,
        total_fees_raw,
        platform_fee_raw,
        creator_fee_raw,
        decoded_flat,
        decoded_flat :consideration AS consideration,
        decoded_flat :offer AS offer,
        seller_address AS seller_address_temp,
        buyer_address AS buyer_address_temp,
        _log_id,
        _inserted_timestamp
    FROM
        mao_combined_base
),
/*

-----
TRANSACTION DATA ENRICHMENT
-----
Joins sales data with transaction metadata (block info, fees, function signatures)

*/
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
        {{ ref('core__fact_transactions') }}
    WHERE
        block_timestamp :: DATE >= '2024-03-15'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                base_sales_buy_and_offer
        )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
/*

-----
NFT TRANSFER EVENT ENRICHMENT
-----
Extracts NFT transfer information from Transfer/TransferSingle events
Used to validate and enrich NFT address information from event logs

*/
nft_transfer_operator AS (
    SELECT
        tx_hash,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        contract_address AS nft_address_from_transfers,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS nft_address_temp,
        --or operator_address
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS offerer,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS recipient,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: STRING AS tokenid,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS erc1155_value
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2024-03-15'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                base_sales_buy_and_offer
        )
        AND topics [0] :: STRING IN (
            '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62',
            '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb'
        )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
/*

-----
FINAL OUTPUT
-----
Assembles all sales data into final output format
Applies deduplication logic and creates unique identifiers

*/
final_seaport AS (
    SELECT
        t.block_number,
        t.block_timestamp,
        s.tx_hash,
        category,
        s.event_index,
        s.contract_address AS platform_address,
        'opensea' AS platform_name,
        'seaport_1_6' AS platform_exchange_version,
        s.event_name,
        offer_length,
        IFF(category IN ('fulfil_oa', 'fulfil_buy'), offerer, seller_address_temp) AS seller_address,
        orderHash,
        IFF(category IN ('fulfil_oa', 'fulfil_buy'), recipient, buyer_address_temp) AS buyer_address,
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
        s.nft_address_temp,
        CASE
            WHEN nft_address_from_transfers IS NOT NULL THEN nft_address_from_transfers
            ELSE s.nft_address_temp
        END AS nft_address,
        s.tokenId,
        s.erc1155_value,
        s.currency_address,
        total_sale_amount_raw AS total_price_raw,
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
            nft_address,
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
        INNER JOIN tx_data t USING (tx_hash)
        LEFT JOIN nft_transfer_operator o USING (
            tx_hash,
            nft_address_temp,
            tokenid,
            recipient
        ) qualify(ROW_NUMBER() over(PARTITION BY nft_log_id
    ORDER BY
        _inserted_timestamp DESC)) = 1
)
SELECT
    *
FROM
    final_seaport
