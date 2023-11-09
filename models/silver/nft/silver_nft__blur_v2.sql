{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH blur_v2_tx AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        _inserted_timestamp,
        _log_id,
        event_name,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS intra_tx_grouping
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_timestamp >= '2023-07-01'
        AND contract_address = '0xb2ecfe4e4d61f8790bbb9de2d1259b9e2410cea5'
        AND event_name IN (
            'Execution721MakerFeePacked',
            'Execution721TakerFeePacked',
            'Execution721Packed'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '24 hours'
    FROM
        {{ this }}
)
{% endif %}
),
blur_v2_traces AS (
    SELECT
        *,
        regexp_substr_all(SUBSTR(input, 11, len(input)), '.{64}') AS segmented_input,
        CASE
            WHEN LEFT(
                input,
                10
            ) = '0x70bce2d6' THEN 'takeAskSingle'
            WHEN LEFT(
                input,
                10
            ) = '0x336d8206' THEN 'takeAskSinglePool'
            WHEN LEFT(
                input,
                10
            ) = '0x3925c3c3' THEN 'takeAsk'
            WHEN LEFT(
                input,
                10
            ) = '0x133ba9a6' THEN 'takeAskPool'
            WHEN LEFT(
                input,
                10
            ) = '0x7034d120' THEN 'takeBid'
            WHEN LEFT(
                input,
                10
            ) = '0xda815cb5' THEN 'takeBidSingle'
        END AS function_name
    FROM
        {{ ref('silver__traces') }}
    WHERE
        block_timestamp >= '2023-07-01'
        AND to_address = '0x5fa60726e62c50af45ff2f6280c468da438a7837'
        AND TYPE = 'DELEGATECALL'
        AND trace_status = 'SUCCESS'
        AND LEFT(
            input,
            10
        ) IN (
            '0x70bce2d6',
            -- takeAskSingle
            '0x336d8206',
            --takeAskSinglePool
            '0x3925c3c3',
            -- takeAsk
            '0x133ba9a6',
            -- takeAskPool
            '0x7034d120',
            -- takeBid
            '0xda815cb5' -- takeBidSingle
        )
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                blur_v2_tx
        ) -- some transactions have calldata to the contract but no actual sale took place. This excludes those tx

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '24 hours'
    FROM
        {{ this }}
)
{% endif %}
),
takeAskSingle_base AS (
    SELECT
        tx_hash,
        function_name,
        'ETH' AS currency_address,
        segmented_input,
        '0x' || SUBSTR(
            segmented_input [2] :: STRING,
            25
        ) AS nft_seller,
        '0x' || SUBSTR(
            segmented_input [3] :: STRING,
            25
        ) AS nft_address,
        segmented_input [4] AS listings_root,
        segmented_input [5] AS number_of_listings,
        segmented_input [6] AS expiration_time,
        CASE
            WHEN utils.udf_hex_to_int(
                segmented_input [7] :: STRING
            ) = 0 THEN 'erc721'
            WHEN utils.udf_hex_to_int(
                segmented_input [7] :: STRING
            ) = 1 THEN 'erc1155'
            ELSE NULL
        END AS asset_type,
        '0x' || SUBSTR(
            segmented_input [8] :: STRING,
            25
        ) AS makerfee_recipient,
        COALESCE(
            utils.udf_hex_to_int(
                segmented_input [9] :: STRING
            ),
            0
        ) / pow(
            10,
            4
        ) AS makerfee_rate,
        segmented_input [10] AS salt,
        utils.udf_hex_to_int(
            segmented_input [19] :: STRING
        ) AS listing_tokenId,
        utils.udf_hex_to_int(
            segmented_input [20] :: STRING
        ) AS listing_amount,
        utils.udf_hex_to_int(
            segmented_input [21] :: STRING
        ) AS listing_price,
        listing_price * makerfee_rate AS creator_fee_raw,
        utils.udf_hex_to_int(
            segmented_input [22] :: STRING
        ) AS exchanges_tokenId,
        utils.udf_hex_to_int(
            segmented_input [23] :: STRING
        ) AS exchanges_amount,
        '0x' || SUBSTR(
            segmented_input [15] :: STRING,
            25
        ) AS nft_receiver -- if it's a blend event, receiver of nft is the blend contract
    FROM
        blur_v2_traces
    WHERE
        function_name = 'takeAskSingle'
),
takeAskSinglePool_base AS (
    SELECT
        tx_hash,
        function_name,
        'ETH' AS currency_address,
        segmented_input,
        utils.udf_hex_to_int(
            segmented_input [2] :: STRING
        ) AS blur_eth_burnt,
        -- amount of blur eth burn
        '0x' || SUBSTR(
            segmented_input [3] :: STRING,
            25
        ) AS nft_seller,
        '0x' || SUBSTR(
            segmented_input [4] :: STRING,
            25
        ) AS nft_address,
        segmented_input [5] AS listings_root,
        segmented_input [6] AS number_of_listings,
        segmented_input [7] AS expiration_time,
        CASE
            WHEN utils.udf_hex_to_int(
                segmented_input [8] :: STRING
            ) = 0 THEN 'erc721'
            WHEN utils.udf_hex_to_int(
                segmented_input [8] :: STRING
            ) = 1 THEN 'erc1155'
            ELSE NULL
        END AS asset_type,
        '0x' || SUBSTR(
            segmented_input [9] :: STRING,
            25
        ) AS makerfee_recipient,
        COALESCE(
            utils.udf_hex_to_int(
                segmented_input [10] :: STRING
            ),
            0
        ) / pow(
            10,
            4
        ) AS makerfee_rate,
        segmented_input [11] AS salt,
        utils.udf_hex_to_int(
            segmented_input [20] :: STRING
        ) AS listing_tokenId,
        utils.udf_hex_to_int(
            segmented_input [21] :: STRING
        ) AS listing_amount,
        utils.udf_hex_to_int(
            segmented_input [22] :: STRING
        ) AS listing_price,
        listing_price * makerfee_rate AS creator_fee_raw,
        utils.udf_hex_to_int(
            segmented_input [23] :: STRING
        ) AS exchanges_tokenId,
        utils.udf_hex_to_int(
            segmented_input [24] :: STRING
        ) AS exchanges_amount,
        '0x' || SUBSTR(
            segmented_input [16] :: STRING,
            25
        ) AS nft_receiver
    FROM
        blur_v2_traces
    WHERE
        function_name = 'takeAskSinglePool'
),
takeAsk_tx AS (
    SELECT
        tx_hash,
        function_name,
        'ETH' AS currency_address,
        segmented_input,
        '0x' || SUBSTR(
            segmented_input [7] :: STRING,
            25
        ) AS nft_buyer,
        utils.udf_hex_to_int(
            segmented_input [8] :: STRING
        ) AS total_order_count,
        -- orders are how an nft is sold. 1 order can have more than 1 nft
        (
            9 + 9 * total_order_count
        ) :: INT AS nft_sale_index,
        -- this gets the index of the array to get the total nft sale count
        utils.udf_hex_to_int(
            segmented_input [nft_sale_index] :: STRING
        ) AS total_nft_sale_count
    FROM
        blur_v2_traces
    WHERE
        function_name = 'takeAsk'
),
takeAsk_flatten_order_raw AS (
    SELECT
        tx_hash,
        nft_buyer,
        segmented_input,
        function_name,
        currency_address,
        total_order_count,
        nft_sale_index,
        total_nft_sale_count,
        INDEX,
        VALUE :: STRING AS VALUE,
        this
    FROM
        takeAsk_tx,
        LATERAL FLATTEN(
            input => segmented_input
        )
),
takeAsk_flattened_order AS (
    SELECT
        *,
        TRUNC(
            INDEX / 9
        ) AS intra_tx_order_group -- each Order has 9 rows of information, so we're creating an index before doing array_agg
    FROM
        takeAsk_flatten_order_raw
    WHERE
        INDEX BETWEEN 9
        AND (
            9 + 9 * total_order_count - 1
        ) -- filtering for the Order section only
),
takeAsk_order_agg AS (
    SELECT
        tx_hash,
        nft_buyer,
        intra_tx_order_group,
        ARRAY_AGG(VALUE) within GROUP (
            ORDER BY
                INDEX,
                intra_tx_order_group ASC
        ) AS segmented_order
    FROM
        takeAsk_flattened_order
    GROUP BY
        ALL
),
takeAsk_order_base AS (
    SELECT
        tx_hash,
        intra_tx_order_group,
        nft_buyer,
        '0x' || SUBSTR(
            segmented_order [0] :: STRING,
            25
        ) AS nft_seller,
        '0x' || SUBSTR(
            segmented_order [1] :: STRING,
            25
        ) AS nft_address,
        segmented_order [2] AS listings_root,
        segmented_order [3] AS number_of_listings,
        segmented_order [4] AS expiration_time,
        CASE
            WHEN utils.udf_hex_to_int(
                segmented_order [5] :: STRING
            ) = 0 THEN 'erc721'
            WHEN utils.udf_hex_to_int(
                segmented_order [5] :: STRING
            ) = 1 THEN 'erc1155'
            ELSE NULL
        END AS asset_type,
        '0x' || SUBSTR(
            segmented_order [6] :: STRING,
            25
        ) AS makerfee_recipient,
        COALESCE(
            utils.udf_hex_to_int(
                segmented_order [7] :: STRING
            ),
            0
        ) / pow(
            10,
            4
        ) AS makerfee_rate,
        segmented_order [8] AS salt
    FROM
        takeAsk_order_agg
),
takeAsk_nft_sale_details_raw AS (
    SELECT
        *,
        INDEX - nft_sale_index AS nft_sale_position_index,
        -- this gives the start of Exchange section. Exchange section specifies for each tokenId, what are their starting index positions in the calldata
        CASE
            WHEN nft_sale_position_index > 0 THEN ((utils.udf_hex_to_int(VALUE) / 32) :: INT)END AS position_raw,
            nft_sale_index + position_raw + 3 AS nft_sale_start_index
            FROM
                takeAsk_flatten_order_raw
            WHERE
                INDEX BETWEEN nft_sale_index
                AND (
                    nft_sale_index + total_nft_sale_count
                )
        ),
        takeAsk_nft_sale_details_final AS (
            SELECT
                *,
                (
                    utils.udf_hex_to_int(
                        this [nft_sale_start_index-2] :: STRING
                    ) + 1
                ) :: INT AS intra_tx_order_group,
                utils.udf_hex_to_int(
                    this [nft_sale_start_index] :: STRING
                ) AS listing_index,
                utils.udf_hex_to_int(
                    this [nft_sale_start_index + 1 ] :: STRING
                ) AS listing_tokenId,
                utils.udf_hex_to_int(
                    this [nft_sale_start_index + 2 ] :: STRING
                ) AS listing_amount,
                utils.udf_hex_to_int(
                    this [nft_sale_start_index + 3 ] :: STRING
                ) AS listing_price,
                utils.udf_hex_to_int(
                    this [nft_sale_start_index + 4 ] :: STRING
                ) AS exchanges_tokenId,
                utils.udf_hex_to_int(
                    this [nft_sale_start_index + 5 ] :: STRING
                ) AS exchanges_amount
            FROM
                takeAsk_nft_sale_details_raw
            WHERE
                nft_sale_start_index IS NOT NULL
        ),
        takeAsk_base AS (
            SELECT
                tx_hash,
                intra_tx_order_group,
                nft_sale_start_index,
                function_name,
                segmented_input,
                currency_address,
                nft_buyer,
                nft_seller,
                nft_address,
                asset_type,
                makerfee_recipient,
                makerfee_rate,
                total_order_count,
                -- nft sale count != total order count means some nfts are batched sold
                total_nft_sale_count,
                INDEX,
                -- position of the segmented input array
                listing_tokenid,
                listing_amount,
                listing_price,
                exchanges_tokenid,
                exchanges_amount,
                listing_price * makerfee_rate AS creator_fee_raw,
                listings_root,
                expiration_time,
                salt
            FROM
                takeAsk_order_base
                LEFT JOIN takeAsk_nft_sale_details_final USING (
                    tx_hash,
                    intra_tx_order_group,
                    nft_buyer
                )
        ),
        takeAskPool_tx AS (
            SELECT
                tx_hash,
                function_name,
                'ETH' AS currency_address,
                segmented_input,
                '0x' || SUBSTR(
                    segmented_input [8] :: STRING,
                    25
                ) AS nft_buyer,
                utils.udf_hex_to_int(
                    segmented_input [2] :: STRING
                ) AS blureth_burnt,
                utils.udf_hex_to_int(
                    segmented_input [9] :: STRING
                ) AS total_order_count,
                (
                    10 + 9 * total_order_count
                ) :: INT AS nft_sale_index,
                utils.udf_hex_to_int(
                    segmented_input [nft_sale_index] :: STRING
                ) AS total_nft_sale_count
            FROM
                blur_v2_traces
            WHERE
                function_name = 'takeAskPool'
        ),
        takeAskPool_flatten_order_raw AS (
            SELECT
                tx_hash,
                function_name,
                segmented_input,
                currency_address,
                nft_buyer,
                blureth_burnt,
                total_order_count,
                nft_sale_index,
                total_nft_sale_count,
                INDEX,
                VALUE :: STRING AS VALUE,
                this
            FROM
                takeAskPool_tx,
                LATERAL FLATTEN(
                    input => segmented_input
                )
        ),
        takeAskPool_flattened_order AS (
            SELECT
                *,
                TRUNC((INDEX -1) / 9) AS intra_tx_order_group -- similar to takeAsk, we're trying to group the Order section before applying array_agg().
            FROM
                takeAskPool_flatten_order_raw
            WHERE
                INDEX BETWEEN 10
                AND (
                    10 + 9 * total_order_count - 1
                )
        ),
        takeAskPool_order_agg AS (
            SELECT
                tx_hash,
                nft_buyer,
                blureth_burnt,
                intra_tx_order_group,
                ARRAY_AGG(VALUE) within GROUP (
                    ORDER BY
                        INDEX,
                        intra_tx_order_group ASC
                ) AS segmented_order
            FROM
                takeAskPool_flattened_order
            GROUP BY
                ALL
        ),
        takeAskPool_order_base AS (
            SELECT
                tx_hash,
                intra_tx_order_group,
                nft_buyer,
                blureth_burnt,
                '0x' || SUBSTR(
                    segmented_order [0] :: STRING,
                    25
                ) AS trader,
                '0x' || SUBSTR(
                    segmented_order [1] :: STRING,
                    25
                ) AS nft_address,
                segmented_order [2] AS listings_root,
                segmented_order [3] AS number_of_listings,
                segmented_order [4] AS expiration_time,
                CASE
                    WHEN utils.udf_hex_to_int(
                        segmented_order [5] :: STRING
                    ) = 0 THEN 'erc721'
                    WHEN utils.udf_hex_to_int(
                        segmented_order [5] :: STRING
                    ) = 1 THEN 'erc1155'
                    ELSE NULL
                END AS asset_type,
                '0x' || SUBSTR(
                    segmented_order [6] :: STRING,
                    25
                ) AS makerfee_recipient,
                COALESCE(
                    utils.udf_hex_to_int(
                        segmented_order [7] :: STRING
                    ),
                    0
                ) / pow(
                    10,
                    4
                ) AS makerfee_rate,
                segmented_order [8] AS salt
            FROM
                takeAskPool_order_agg
        ),
        takeAskPool_nft_sale_details_raw AS (
            SELECT
                *,
                INDEX - nft_sale_index AS nft_sale_position_index,
                CASE
                    WHEN nft_sale_position_index > 0 THEN ((utils.udf_hex_to_int(VALUE) / 32) :: INT)END AS position_raw,
                    nft_sale_index + position_raw + 3 AS nft_sale_start_index
                    FROM
                        takeAskPool_flatten_order_raw
                    WHERE
                        INDEX BETWEEN nft_sale_index
                        AND (
                            nft_sale_index + total_nft_sale_count
                        )
                ),
                takeAskPool_nft_sale_details_final AS (
                    SELECT
                        *,
                        (
                            utils.udf_hex_to_int(
                                this [nft_sale_start_index-2] :: STRING
                            ) + 1
                        ) :: INT AS intra_tx_order_group,
                        utils.udf_hex_to_int(
                            this [nft_sale_start_index] :: STRING
                        ) AS listing_index,
                        utils.udf_hex_to_int(
                            this [nft_sale_start_index + 1 ] :: STRING
                        ) AS listing_tokenId,
                        utils.udf_hex_to_int(
                            this [nft_sale_start_index + 2 ] :: STRING
                        ) AS listing_amount,
                        utils.udf_hex_to_int(
                            this [nft_sale_start_index + 3 ] :: STRING
                        ) AS listing_price,
                        utils.udf_hex_to_int(
                            this [nft_sale_start_index + 4 ] :: STRING
                        ) AS exchanges_tokenId,
                        utils.udf_hex_to_int(
                            this [nft_sale_start_index + 5 ] :: STRING
                        ) AS exchanges_amount
                    FROM
                        takeAskPool_nft_sale_details_raw
                    WHERE
                        nft_sale_start_index IS NOT NULL
                ),
                takeAskPool_base AS (
                    SELECT
                        tx_hash,
                        function_name,
                        segmented_input,
                        blureth_burnt,
                        currency_address,
                        intra_tx_order_group,
                        nft_buyer,
                        trader AS nft_seller,
                        nft_address,
                        asset_type,
                        makerfee_recipient,
                        makerfee_rate,
                        total_order_count,
                        -- nft sale count != total order count means some nfts are batched sold
                        total_nft_sale_count,
                        INDEX,
                        -- position of the segmented input array
                        listing_tokenid,
                        listing_amount,
                        listing_price,
                        exchanges_tokenid,
                        exchanges_amount,
                        listing_price * makerfee_rate AS creator_fee_raw,
                        listings_root,
                        expiration_time,
                        salt,
                        nft_sale_start_index
                    FROM
                        takeAskPool_order_base
                        LEFT JOIN takeAskPool_nft_sale_details_final USING (
                            tx_hash,
                            intra_tx_order_group,
                            nft_buyer,
                            blureth_burnt
                        )
                ),
                takeBid_tx AS (
                    SELECT
                        tx_hash,
                        function_name,
                        '0x0000000000a39bb272e79075ade125fd351887ac' AS currency_address,
                        -- blur eth
                        segmented_input,
                        '0x' || SUBSTR(
                            segmented_input [4] :: STRING,
                            25
                        ) AS royalty_receiver,
                        COALESCE(
                            utils.udf_hex_to_int(
                                segmented_input [5] :: STRING
                            ),
                            0
                        ) / pow(
                            10,
                            4
                        ) AS royalty_rate,
                        utils.udf_hex_to_int(
                            segmented_input [7] :: STRING
                        ) AS total_order_count,
                        (
                            8 + 9 * total_order_count
                        ) :: INT AS nft_sale_index,
                        utils.udf_hex_to_int(
                            segmented_input [nft_sale_index] :: STRING
                        ) AS total_nft_sale_count
                    FROM
                        blur_v2_traces
                    WHERE
                        function_name = 'takeBid'
                ),
                takeBid_flatten_order_raw AS (
                    SELECT
                        tx_hash,
                        segmented_input,
                        function_name,
                        currency_address,
                        royalty_receiver,
                        royalty_rate,
                        total_order_count,
                        nft_sale_index,
                        total_nft_sale_count,
                        INDEX,
                        VALUE :: STRING AS VALUE,
                        this
                    FROM
                        takeBid_tx,
                        LATERAL FLATTEN(
                            input => segmented_input
                        )
                ),
                takeBid_flattened_order AS (
                    SELECT
                        *,
                        TRUNC((INDEX + 1) / 9) AS intra_tx_order_group
                    FROM
                        takeBid_flatten_order_raw
                    WHERE
                        INDEX BETWEEN 8
                        AND (
                            8 + 9 * total_order_count - 1
                        )
                ),
                takeBid_order_agg AS (
                    SELECT
                        tx_hash,
                        royalty_receiver,
                        royalty_rate,
                        intra_tx_order_group,
                        ARRAY_AGG(VALUE) within GROUP (
                            ORDER BY
                                INDEX,
                                intra_tx_order_group ASC
                        ) AS segmented_order
                    FROM
                        takeBid_flattened_order
                    GROUP BY
                        ALL
                ),
                takeBid_order_base AS (
                    SELECT
                        tx_hash,
                        intra_tx_order_group,
                        royalty_receiver,
                        royalty_rate,
                        '0x' || SUBSTR(
                            segmented_order [0] :: STRING,
                            25
                        ) AS nft_receiver,
                        '0x' || SUBSTR(
                            segmented_order [1] :: STRING,
                            25
                        ) AS nft_address,
                        segmented_order [2] AS listings_root,
                        segmented_order [3] AS number_of_listings,
                        segmented_order [4] AS expiration_time,
                        CASE
                            WHEN utils.udf_hex_to_int(
                                segmented_order [5] :: STRING
                            ) = 0 THEN 'erc721'
                            WHEN utils.udf_hex_to_int(
                                segmented_order [5] :: STRING
                            ) = 1 THEN 'erc1155'
                            ELSE NULL
                        END AS asset_type,
                        '0x' || SUBSTR(
                            segmented_order [6] :: STRING,
                            25
                        ) AS makerfee_recipient,
                        COALESCE(
                            utils.udf_hex_to_int(
                                segmented_order [7] :: STRING
                            ),
                            0
                        ) / pow(
                            10,
                            4
                        ) AS makerfee_rate,
                        segmented_order [8] AS salt
                    FROM
                        takeBid_order_agg
                ),
                takeBid_nft_sale_details_raw AS (
                    SELECT
                        *,
                        INDEX - nft_sale_index AS nft_sale_position_index,
                        CASE
                            WHEN nft_sale_position_index > 0 THEN ((utils.udf_hex_to_int(VALUE) / 32) :: INT)END AS position_raw,
                            nft_sale_index + position_raw + 3 AS nft_sale_start_index
                            FROM
                                takeBid_flatten_order_raw
                            WHERE
                                INDEX BETWEEN nft_sale_index
                                AND (
                                    nft_sale_index + total_nft_sale_count
                                )
                        ),
                        takeBid_nft_sale_details_final AS (
                            SELECT
                                *,
                                (
                                    utils.udf_hex_to_int(
                                        this [nft_sale_start_index-2] :: STRING
                                    ) + 1
                                ) :: INT AS intra_tx_order_group,
                                utils.udf_hex_to_int(
                                    this [nft_sale_start_index] :: STRING
                                ) AS listing_index,
                                utils.udf_hex_to_int(
                                    this [nft_sale_start_index + 1 ] :: STRING
                                ) AS listing_tokenId,
                                utils.udf_hex_to_int(
                                    this [nft_sale_start_index + 2 ] :: STRING
                                ) AS listing_amount,
                                utils.udf_hex_to_int(
                                    this [nft_sale_start_index + 3 ] :: STRING
                                ) AS listing_price,
                                utils.udf_hex_to_int(
                                    this [nft_sale_start_index + 4 ] :: STRING
                                ) AS exchanges_tokenId,
                                utils.udf_hex_to_int(
                                    this [nft_sale_start_index + 5 ] :: STRING
                                ) AS exchanges_amount,
                                listing_price * royalty_rate AS creator_fee_raw
                            FROM
                                takeBid_nft_sale_details_raw
                            WHERE
                                nft_sale_start_index IS NOT NULL
                        ),
                        takeBid_base_raw AS (
                            SELECT
                                tx_hash,
                                function_name,
                                segmented_input,
                                currency_address,
                                royalty_receiver,
                                royalty_rate,
                                intra_tx_order_group,
                                nft_receiver AS nft_to_address,
                                nft_address,
                                asset_type,
                                total_order_count,
                                -- nft sale count != total order count means some nfts are batched sold
                                total_nft_sale_count,
                                INDEX,
                                -- position of the segmented input array
                                listing_price,
                                exchanges_tokenid AS tokenid,
                                exchanges_amount,
                                creator_fee_raw,
                                listings_root,
                                expiration_time,
                                salt,
                                nft_sale_start_index
                            FROM
                                takeBid_order_base
                                LEFT JOIN takeBid_nft_sale_details_final USING (
                                    tx_hash,
                                    intra_tx_order_group
                                )
                        ),
                        takeBidSingle_base_raw AS (
                            -- only single bids
                            SELECT
                                tx_hash,
                                function_name,
                                '0x0000000000a39bb272e79075ade125fd351887ac' AS currency_address,
                                -- blur eth
                                segmented_input,
                                '0x' || SUBSTR(
                                    segmented_input [2] :: STRING,
                                    25
                                ) AS nft_to_address,
                                '0x' || SUBSTR(
                                    segmented_input [3] :: STRING,
                                    25
                                ) AS nft_address,
                                segmented_input [4] AS listings_root,
                                segmented_input [5] AS number_of_listings,
                                segmented_input [6] AS expiration_time,
                                CASE
                                    WHEN utils.udf_hex_to_int(
                                        segmented_input [7] :: STRING
                                    ) = 0 THEN 'erc721'
                                    WHEN utils.udf_hex_to_int(
                                        segmented_input [7] :: STRING
                                    ) = 1 THEN 'erc1155'
                                    ELSE NULL
                                END AS asset_type,
                                '0x' || SUBSTR(
                                    segmented_input [8] :: STRING,
                                    25
                                ) AS makerfee_recipient,
                                COALESCE(
                                    utils.udf_hex_to_int(
                                        segmented_input [9] :: STRING
                                    ),
                                    0
                                ) / pow(
                                    10,
                                    4
                                ) AS makerfee_rate,
                                segmented_input [10] AS salt,
                                '0x' || SUBSTR(
                                    segmented_input [12] :: STRING,
                                    25
                                ) AS takerfee_recipient,
                                COALESCE(
                                    utils.udf_hex_to_int(
                                        segmented_input [13] :: STRING
                                    ),
                                    0
                                ) / pow(
                                    10,
                                    4
                                ) AS takerfee_rate,
                                utils.udf_hex_to_int(
                                    segmented_input [20] :: STRING
                                ) AS listing_price,
                                utils.udf_hex_to_int(
                                    segmented_input [21] :: STRING
                                ) AS tokenid,
                                utils.udf_hex_to_int(
                                    segmented_input [22] :: STRING
                                ) AS taker_amount,
                                listing_price * takerfee_rate AS creator_fee_raw
                            FROM
                                blur_v2_traces
                            WHERE
                                function_name = 'takeBidSingle'
                        ),
                        nft_transfers AS (
                            SELECT
                                tx_hash,
                                from_address AS nft_from_address,
                                to_address AS nft_to_address,
                                contract_address AS nft_address,
                                tokenid
                            FROM
                                {{ ref('silver__nft_transfers') }}
                            WHERE
                                block_timestamp >= '2023-07-01'
                                AND (
                                    tx_hash IN (
                                        SELECT
                                            tx_hash
                                        FROM
                                            takeBid_base_raw
                                    )
                                    OR tx_hash IN (
                                        SELECT
                                            tx_hash
                                        FROM
                                            takeBidSingle_base_raw
                                    )
                                )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '24 hours'
    FROM
        {{ this }}
)
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY tx_hash,
    nft_address,
    tokenid,
    nft_to_address
    ORDER BY
        event_index DESC
) = 1
),
takeBid_base AS (
    SELECT
        *
    FROM
        takeBid_base_raw
        INNER JOIN nft_transfers USING (
            tx_hash,
            nft_to_address,
            nft_address,
            tokenid
        )
),
takeBidSingle_base AS (
    SELECT
        *
    FROM
        takeBidSingle_base_raw
        INNER JOIN nft_transfers USING (
            tx_hash,
            nft_to_address,
            nft_address,
            tokenid
        )
),
all_base AS (
    SELECT
        tx_hash,
        function_name,
        segmented_input,
        '0' AS total_blur_eth_burnt,
        nft_seller AS seller_address,
        nft_receiver AS buyer_address,
        nft_address,
        listing_tokenId AS tokenid,
        IFF(
            asset_type = 'erc1155',
            listing_amount,
            NULL
        ) AS erc1155_value,
        asset_type,
        currency_address,
        listing_price AS total_price_raw,
        creator_fee_raw AS total_fees_raw,
        0 AS platform_fee_raw,
        creator_fee_raw,
        makerfee_recipient AS creator_fee_recipient,
        makerfee_rate AS creator_fee_rate,
        listings_root,
        expiration_time,
        salt,
        1 AS intra_tx_grouping
    FROM
        takeAskSingle_base -- single NFT buys using ETH
    UNION ALL
    SELECT
        tx_hash,
        function_name,
        segmented_input,
        blur_eth_burnt :: STRING AS total_blur_eth_burnt,
        -- total amount of blur eth burn in the tx if any, then eth sent from blur bidding to blur marketplace
        nft_seller AS seller_address,
        nft_receiver AS buyer_address,
        nft_address,
        listing_tokenId AS tokenid,
        IFF(
            asset_type = 'erc1155',
            listing_amount,
            NULL
        ) AS erc1155_value,
        asset_type,
        currency_address,
        listing_price AS total_price_raw,
        creator_fee_raw AS total_fees_raw,
        0 AS platform_fee_raw,
        creator_fee_raw,
        makerfee_recipient AS creator_fee_recipient,
        makerfee_rate AS creator_fee_rate,
        listings_root,
        expiration_time,
        salt,
        1 AS intra_tx_grouping
    FROM
        takeAskSinglePool_base -- single NFT buys with blur ETH. Seller receives ETH, buyer pays with Blur ETH that is burnt
    UNION ALL
    SELECT
        tx_hash,
        function_name,
        segmented_input,
        '0' AS total_blur_eth_burnt,
        nft_seller AS seller_address,
        nft_buyer AS buyer_address,
        nft_address,
        listing_tokenid AS tokenid,
        IFF(
            asset_type = 'erc1155',
            listing_amount,
            NULL
        ) AS erc1155_value,
        asset_type,
        currency_address,
        listing_price AS total_price_raw,
        creator_fee_raw AS total_fees_raw,
        0 AS platform_fee_raw,
        creator_fee_raw,
        makerfee_recipient AS creator_fee_recipient,
        makerfee_rate AS creator_fee_rate,
        listings_root,
        expiration_time,
        salt,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                nft_sale_start_index ASC
        ) AS intra_tx_grouping
    FROM
        takeAsk_base -- multiple NFT buys with ETH only
    UNION ALL
    SELECT
        tx_hash,
        function_name,
        segmented_input,
        blureth_burnt :: STRING AS total_blur_eth_burnt,
        nft_seller AS seller_address,
        nft_buyer AS buyer_address,
        nft_address,
        listing_tokenid AS tokenid,
        IFF(
            asset_type = 'erc1155',
            listing_amount,
            NULL
        ) AS erc1155_value,
        asset_type,
        currency_address,
        listing_price AS total_price_raw,
        creator_fee_raw AS total_fees_raw,
        0 AS platform_fee_raw,
        creator_fee_raw,
        makerfee_recipient AS creator_fee_recipient,
        makerfee_rate AS creator_fee_rate,
        listings_root,
        expiration_time,
        salt,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                nft_sale_start_index ASC
        ) AS intra_tx_grouping
    FROM
        takeAskPool_base -- multiple NFT buys with Blur ETH, but blur eth is burnt. NFT seller receives ETH
    UNION ALL
    SELECT
        tx_hash,
        function_name,
        segmented_input,
        '0' AS total_blur_eth_burnt,
        -- because seller receives blur eth directly. no blur eth burnt to eth
        nft_from_address AS seller_address,
        nft_to_address AS buyer_address,
        nft_address,
        tokenid,
        IFF(
            asset_type = 'erc1155',
            exchanges_amount,
            NULL
        ) AS erc1155_value,
        asset_type,
        currency_address,
        listing_price AS total_price_raw,
        creator_fee_raw AS total_fees_raw,
        0 AS platform_fee_raw,
        creator_fee_raw,
        royalty_receiver AS creator_fee_recipient,
        royalty_rate AS creator_fee_rate,
        listings_root,
        expiration_time,
        salt,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                nft_sale_start_index ASC
        ) AS intra_tx_grouping
    FROM
        takeBid_base -- multiple NFT buys using Blur ETH . seller receives Blur ETH
    UNION ALL
    SELECT
        tx_hash,
        function_name,
        segmented_input,
        '0' AS total_blur_eth_burnt,
        nft_from_address AS seller_address,
        nft_to_address AS buyer_address,
        nft_address,
        tokenid,
        IFF(
            asset_type = 'erc1155',
            taker_amount,
            NULL
        ) AS erc1155_value,
        asset_type,
        currency_address,
        listing_price AS total_price_raw,
        creator_fee_raw AS total_fees_raw,
        0 AS platform_fee_raw,
        creator_fee_raw,
        takerfee_recipient AS creator_fee_recipient,
        takerfee_rate AS creator_fee_rate,
        listings_root,
        expiration_time,
        salt,
        1 AS intra_tx_grouping
    FROM
        takeBidSingle_base -- single NFT buys using Blur ETH. seller receives Blur ETH
),
all_base_x_logs AS (
    SELECT
        *
    FROM
        all_base
        INNER JOIN blur_v2_tx USING (
            tx_hash,
            intra_tx_grouping
        )
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
        block_timestamp :: DATE >= '2023-07-01'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                all_base_x_logs
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '24 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    CASE
        WHEN function_name IN (
            'takeBid',
            'takeBidSingle'
        ) THEN 'bid_won'
        ELSE 'sale'
    END AS event_type,
    event_name,
    function_name,
    '0xb2ecfe4e4d61f8790bbb9de2d1259b9e2410cea5' AS platform_address,
    'blur' AS platform_name,
    'blur v2' AS platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address,
    asset_type,
    erc1155_value,
    tokenid AS tokenId,
    currency_address,
    TRY_TO_NUMBER(total_price_raw) AS total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    creator_fee_recipient,
    creator_fee_rate,
    total_blur_eth_burnt,
    tx_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    intra_tx_grouping,
    segmented_input,
    listings_root,
    expiration_time,
    salt,
    _log_id,
    input_data,
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
    all_base_x_logs
    INNER JOIN tx_data USING (tx_hash) qualify (ROW_NUMBER() over(PARTITION BY nft_log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
