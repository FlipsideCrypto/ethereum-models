{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','nft','curated']
) }}

WITH raw AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        tx_position,
        trace_index,    
        from_address,
        from_address_name,
        to_address,
        to_address_name,
        input,
        output,
        function_name,
        full_decoded_trace,
        full_decoded_trace AS decoded_data,
        decoded_input_data,
        decoded_output_data,
        TYPE,
        sub_traces,
        VALUE,
        value_precise_raw,
        value_precise,
        gas,
        gas_used,
        trace_succeeded,
        error_reason,
        tx_succeeded, 
        fact_decoded_traces_id,
        inserted_timestamp,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__ez_decoded_traces') }}
    WHERE
        block_timestamp :: DATE >= '2021-06-01'
        AND to_address = '0x9757f2d2b135150bbeb65308d4a91804107cd8d6'
        AND function_name IN (
            'matchOrders',
            'directAcceptBid',
            'directPurchase'
        )
        AND tx_succeeded
        AND trace_succeeded

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
direct_purchase_raw AS (
    SELECT
        block_timestamp,
        function_name,
        tx_hash,
        trace_index,
        decoded_data :decoded_input_data :direct AS buy,
        from_address AS buyer_address,
        to_address AS platform_address,
        buy :sellOrderMaker :: STRING AS seller_address,
        CASE
            WHEN buy :nftAssetClass :: STRING = '73ad2146' THEN 'ERC721'
            WHEN buy :nftAssetClass :: STRING = '973bb640' THEN 'ERC1155'
            WHEN buy :nftAssetClass :: STRING IN (
                'd8f960c1',
                '1cdfaa40'
            ) THEN 'nft_params'
        END AS nft_standard_raw,
        CASE
            WHEN nft_standard_raw = 'nft_params' THEN regexp_substr_all(
                SUBSTR(buy :nftData :: STRING, 1, len(buy :nftData)),
                '.{64}'
            )
            ELSE NULL
        END AS segmented_data,
        CASE
            WHEN nft_standard_raw = 'nft_params' THEN utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) / 32
            ELSE NULL
        END AS tokenid_pad,
        CASE
            WHEN nft_standard_raw = 'nft_params' THEN utils.udf_hex_to_int(
                segmented_data [tokenid_pad]
            ) :: STRING
            ELSE utils.udf_hex_to_int(SUBSTR(buy :nftData, 89, 40)) :: STRING
        END AS tokenid,
        '0x' || SUBSTR(
            buy :nftData,
            25,
            40
        ) :: STRING AS nft_address,
        IFF(
            buy :nftAssetClass :: STRING IN (
                '973bb640',
                '1cdfaa40'
            ),
            buy :buyOrderNftAmount :: STRING,
            NULL
        ) AS erc1155_value,
        IFF(
            buy :paymentToken :: STRING = '0x0000000000000000000000000000000000000000',
            'ETH',
            buy :paymentToken :: STRING
        ) AS currency_address,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) AS intra_grouping,
        'sale' AS event_type
    FROM
        raw
    WHERE
        function_name = 'directPurchase'
),
direct_accept_bid_raw AS (
    SELECT
        tx_hash,
        trace_index,
        block_timestamp,
        function_name,
        decoded_data :decoded_input_data :direct AS bid,
        from_address AS seller_address,
        to_address AS platform_address,
        bid :bidMaker :: STRING AS buyer_address,
        CASE
            WHEN bid :nftAssetClass :: STRING = '73ad2146' THEN 'ERC721'
            WHEN bid :nftAssetClass :: STRING = '973bb640' THEN 'ERC1155'
            WHEN bid :nftAssetClass :: STRING IN (
                'd8f960c1',
                '1cdfaa40'
            ) THEN 'nft_params'
        END AS nft_standard_raw,
        CASE
            WHEN nft_standard_raw = 'nft_params' THEN regexp_substr_all(
                SUBSTR(bid :nftData :: STRING, 1, len(bid :nftData)),
                '.{64}'
            )
            ELSE NULL
        END AS segmented_data,
        CASE
            WHEN nft_standard_raw = 'nft_params' THEN utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) / 32
            ELSE NULL
        END AS tokenid_pad,
        CASE
            WHEN nft_standard_raw = 'nft_params' THEN utils.udf_hex_to_int(
                segmented_data [tokenid_pad]
            ) :: STRING
            ELSE utils.udf_hex_to_int(SUBSTR(bid :nftData, 89, 40)) :: STRING
        END AS tokenid,
        '0x' || SUBSTR(
            bid :nftData,
            25,
            40
        ) :: STRING AS nft_address,
        IFF(
            bid :nftAssetClass :: STRING IN (
                '973bb640',
                '1cdfaa40'
            ),
            bid :bidNftAmount :: STRING,
            NULL
        ) AS erc1155_value,
        IFF(
            bid :paymentToken :: STRING = '0x0000000000000000000000000000000000000000',
            'ETH',
            bid :paymentToken :: STRING
        ) AS currency_address,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) AS intra_grouping,
        'bid_won' AS event_type
    FROM
        raw
    WHERE
        function_name = 'directAcceptBid'
),
match_orders_raw AS (
    SELECT
        function_name,
        decoded_data,
        decoded_data :decoded_input_data :orderLeft AS order_left,
        order_left :maker :: STRING AS left_maker_address,
        order_left :taker :: STRING AS left_taker_address,
        CASE
            WHEN order_left :makeAsset :assetType :assetClass :: STRING = 'aaaebeba' THEN 'ETH'
            WHEN order_left :makeAsset :assetType :assetClass :: STRING = '8ae85d84' THEN 'ERC20'
            WHEN order_left :makeAsset :assetType :assetClass :: STRING = '73ad2146' THEN 'ERC721'
            WHEN order_left :makeAsset :assetType :assetClass :: STRING = '973bb640' THEN 'ERC1155'
            WHEN order_left :makeAsset :assetType :assetClass :: STRING IN (
                'd8f960c1',
                '1cdfaa40'
            ) THEN 'nft_params'
        END AS left_maker_asset_class,
        CASE
            WHEN left_maker_asset_class = 'nft_params' THEN regexp_substr_all(
                SUBSTR(
                    order_left :makeAsset :assetType :data :: STRING,
                    1
                ),
                '.{64}'
            )
            ELSE NULL
        END AS left_maker_segmented_data,
        CASE
            WHEN left_maker_asset_class = 'ETH' THEN 'ETH'
            WHEN left_maker_asset_class = 'ERC20' THEN '0x' || SUBSTR(
                order_left :makeAsset :assetType :data :: STRING,
                25
            )
            ELSE NULL
        END AS left_maker_currency_address,
        CASE
            WHEN left_maker_asset_class IN (
                'ERC721',
                'ERC1155',
                'nft_params'
            ) THEN '0x' || SUBSTR(
                order_left :makeAsset :assetType :data :: STRING,
                25,
                40
            )
            ELSE NULL
        END AS left_maker_nft_address,
        CASE
            WHEN left_maker_asset_class = 'nft_params' THEN utils.udf_hex_to_int(
                left_maker_segmented_data [1] :: STRING
            ) / 32
            ELSE NULL
        END AS left_make_tokenid_pad,
        CASE
            WHEN left_maker_asset_class = 'nft_params' THEN utils.udf_hex_to_int(
                left_maker_segmented_data [left_make_tokenid_pad]
            ) :: STRING
            WHEN left_maker_asset_class IN (
                'ERC721',
                'ERC1155'
            ) THEN utils.udf_hex_to_int(
                SUBSTR(
                    order_left :makeAsset :assetType :data :: STRING,
                    89,
                    40
                )
            ) :: STRING
            ELSE NULL
        END AS left_maker_tokenid,
        IFF(
            order_left :makeAsset :assetType :assetClass :: STRING IN (
                '973bb640',
                '1cdfaa40'
            ),
            order_left :makeAsset :value :: STRING,
            NULL
        ) AS left_maker_erc1155_value,
        -- left make
        -- left take
        CASE
            WHEN order_left :takeAsset :assetType :assetClass :: STRING = 'aaaebeba' THEN 'ETH'
            WHEN order_left :takeAsset :assetType :assetClass :: STRING = '8ae85d84' THEN 'ERC20'
            WHEN order_left :takeAsset :assetType :assetClass :: STRING = '73ad2146' THEN 'ERC721'
            WHEN order_left :takeAsset :assetType :assetClass :: STRING = '973bb640' THEN 'ERC1155'
            WHEN order_left :makeAsset :assetType :assetClass :: STRING IN (
                'd8f960c1',
                '1cdfaa40'
            ) THEN 'nft_params'
        END AS left_taker_asset_class,
        CASE
            WHEN left_taker_asset_class = 'nft_params' THEN regexp_substr_all(
                SUBSTR(
                    order_left :takeAsset :assetType :data :: STRING,
                    1
                ),
                '.{64}'
            )
            ELSE NULL
        END AS left_taker_segmented_data,
        CASE
            WHEN left_taker_asset_class = 'ETH' THEN 'ETH'
            WHEN left_taker_asset_class = 'ERC20' THEN '0x' || SUBSTR(
                order_left :takeAsset :assetType :data :: STRING,
                25
            )
            ELSE NULL
        END AS left_taker_currency_address,
        CASE
            WHEN left_taker_asset_class IN (
                'ERC721',
                'ERC1155',
                'nft_params'
            ) THEN '0x' || SUBSTR(
                order_left :takeAsset :assetType :data :: STRING,
                25,
                40
            )
            ELSE NULL
        END AS left_taker_nft_address,
        CASE
            WHEN left_taker_asset_class = 'nft_params' THEN utils.udf_hex_to_int(
                left_taker_segmented_data [1] :: STRING
            ) / 32
            ELSE NULL
        END AS left_take_tokenid_pad,
        CASE
            WHEN left_taker_asset_class = 'nft_params' THEN utils.udf_hex_to_int(
                left_taker_segmented_data [left_take_tokenid_pad]
            ) :: STRING
            WHEN left_taker_asset_class IN (
                'ERC721',
                'ERC1155'
            ) THEN utils.udf_hex_to_int(
                SUBSTR(
                    order_left :takeAsset :assetType :data :: STRING,
                    89,
                    40
                )
            ) :: STRING
            ELSE NULL
        END AS left_taker_tokenid,
        IFF(
            order_left :takeAsset :assetType :assetClass :: STRING IN (
                '973bb640',
                '1cdfaa40'
            ),
            order_left :takeAsset :value :: STRING,
            NULL
        ) AS left_taker_erc1155_value,
        -- right
        decoded_data :decoded_input_data :orderRight AS order_right,
        order_right :maker :: STRING AS right_maker_address,
        order_right :taker :: STRING AS right_taker_address,
        CASE
            WHEN order_right :makeAsset :assetType :assetClass :: STRING = 'aaaebeba' THEN 'ETH'
            WHEN order_right :makeAsset :assetType :assetClass :: STRING = '8ae85d84' THEN 'ERC20'
            WHEN order_right :makeAsset :assetType :assetClass :: STRING = '73ad2146' THEN 'ERC721'
            WHEN order_right :makeAsset :assetType :assetClass :: STRING = '973bb640' THEN 'ERC1155'
            WHEN order_right :makeAsset :assetType :assetClass :: STRING IN (
                'd8f960c1',
                '1cdfaa40'
            ) THEN 'nft_params'
        END AS right_maker_asset_class,
        CASE
            WHEN right_maker_asset_class = 'nft_params' THEN regexp_substr_all(
                SUBSTR(
                    order_right :makeAsset :assetType :data :: STRING,
                    1
                ),
                '.{64}'
            )
            ELSE NULL
        END AS right_maker_segmented_data,
        CASE
            WHEN right_maker_asset_class = 'ETH' THEN 'ETH'
            WHEN right_maker_asset_class = 'ERC20' THEN '0x' || SUBSTR(
                order_right :makeAsset :assetType :data :: STRING,
                25
            )
            ELSE NULL
        END AS right_maker_currency_address,
        CASE
            WHEN right_maker_asset_class IN (
                'ERC721',
                'ERC1155',
                'nft_params'
            ) THEN '0x' || SUBSTR(
                order_right :makeAsset :assetType :data :: STRING,
                25,
                40
            )
            ELSE NULL
        END AS right_maker_nft_address,
        CASE
            WHEN right_maker_asset_class = 'nft_params' THEN utils.udf_hex_to_int(
                right_maker_segmented_data [1] :: STRING
            ) / 32
            ELSE NULL
        END AS right_make_tokenid_pad,
        CASE
            WHEN right_maker_asset_class = 'nft_params' THEN utils.udf_hex_to_int(
                right_maker_segmented_data [right_make_tokenid_pad]
            ) :: STRING
            WHEN right_maker_asset_class IN (
                'ERC721',
                'ERC1155'
            ) THEN utils.udf_hex_to_int(
                SUBSTR(
                    order_right :makeAsset :assetType :data :: STRING,
                    89,
                    40
                )
            ) :: STRING
            ELSE NULL
        END AS right_maker_tokenid,
        IFF(
            order_right :makeAsset :assetType :assetClass :: STRING IN (
                '973bb640',
                '1cdfaa40'
            ),
            order_right :makeAsset :value :: STRING,
            NULL
        ) AS right_maker_erc1155_value,
        -- right make
        -- right take
        CASE
            WHEN order_right :takeAsset :assetType :assetClass :: STRING = 'aaaebeba' THEN 'ETH'
            WHEN order_right :takeAsset :assetType :assetClass :: STRING = '8ae85d84' THEN 'ERC20'
            WHEN order_right :takeAsset :assetType :assetClass :: STRING = '73ad2146' THEN 'ERC721'
            WHEN order_right :takeAsset :assetType :assetClass :: STRING = '973bb640' THEN 'ERC1155'
            WHEN order_right :takeAsset :assetType :assetClass :: STRING IN (
                'd8f960c1',
                '1cdfaa40'
            ) THEN 'nft_params'
        END AS right_taker_asset_class,
        CASE
            WHEN right_taker_asset_class = 'nft_params' THEN regexp_substr_all(
                SUBSTR(
                    order_right :takeAsset :assetType :data :: STRING,
                    1
                ),
                '.{64}'
            )
            ELSE NULL
        END AS right_taker_segmented_data,
        CASE
            WHEN right_taker_asset_class = 'ETH' THEN 'ETH'
            WHEN right_taker_asset_class = 'ERC20' THEN '0x' || SUBSTR(
                order_right :takeAsset :assetType :data :: STRING,
                25
            )
            ELSE NULL
        END AS right_taker_currency_address,
        CASE
            WHEN right_taker_asset_class IN (
                'ERC721',
                'ERC1155',
                'nft_params'
            ) THEN '0x' || SUBSTR(
                order_right :takeAsset :assetType :data :: STRING,
                25,
                40
            )
            ELSE NULL
        END AS right_taker_nft_address,
        CASE
            WHEN right_taker_asset_class = 'nft_params' THEN utils.udf_hex_to_int(
                right_taker_segmented_data [1] :: STRING
            ) / 32
            ELSE NULL
        END AS right_take_tokenid_pad,
        CASE
            WHEN right_taker_asset_class = 'nft_params' THEN utils.udf_hex_to_int(
                right_taker_segmented_data [right_take_tokenid_pad]
            ) :: STRING
            WHEN right_taker_asset_class IN (
                'ERC721',
                'ERC1155'
            ) THEN utils.udf_hex_to_int(
                SUBSTR(
                    order_right :takeAsset :assetType :data :: STRING,
                    89,
                    40
                )
            ) :: STRING
            ELSE NULL
        END AS right_taker_tokenid,
        IFF(
            order_right :takeAsset :assetType :assetClass :: STRING IN (
                '973bb640',
                '1cdfaa40'
            ),
            order_right :takeAsset :value :: STRING,
            NULL
        ) AS right_taker_erc1155_value,
        --- final left
        COALESCE(
            left_maker_currency_address,
            left_taker_currency_address
        ) AS left_currency_address,
        IFF(
            left_maker_currency_address IS NULL,
            left_maker_address,
            left_taker_address
        ) AS left_seller_address,
        -- there will still be 0x0000 ones
        IFF(
            left_maker_currency_address IS NULL,
            left_taker_address,
            left_maker_address
        ) AS left_buyer_address,
        COALESCE(
            left_maker_nft_address,
            left_taker_nft_address
        ) AS left_nft_address,
        COALESCE(
            left_maker_tokenid,
            left_taker_tokenid
        ) AS left_tokenid,
        --- final right
        COALESCE(
            right_maker_currency_address,
            right_taker_currency_address
        ) AS right_currency_address,
        IFF(
            right_maker_currency_address IS NULL,
            right_maker_address,
            right_taker_address
        ) AS right_seller_address,
        -- there will still be 0x0000 ones
        IFF(
            right_maker_currency_address IS NULL,
            right_taker_address,
            right_maker_address
        ) AS right_buyer_address,
        COALESCE(
            right_maker_nft_address,
            right_taker_nft_address
        ) AS right_nft_address,
        COALESCE(
            right_maker_tokenid,
            right_taker_tokenid
        ) AS right_tokenid,
        -- final
        COALESCE(
            left_currency_address,
            right_currency_address
        ) AS currency_address,
        COALESCE(
            left_nft_address,
            right_nft_address
        ) AS nft_address,
        COALESCE(
            left_tokenid,
            right_tokenid
        ) AS tokenid,
        CASE
            WHEN left_taker_asset_class IN (
                'nft_params',
                'ERC1155'
            ) THEN left_taker_erc1155_value
            ELSE right_taker_erc1155_value
        END AS erc1155_value,
        IFF(
            left_seller_address != '0x0000000000000000000000000000000000000000',
            left_seller_address,
            right_seller_address
        ) AS seller_address,
        IFF(
            left_buyer_address != '0x0000000000000000000000000000000000000000',
            left_buyer_address,
            right_buyer_address
        ) AS buyer_address,
        IFF(
            currency_address = 'ETH',
            'sale',
            'bid_won'
        ) AS event_type,
        tx_hash,
        trace_index,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) AS intra_grouping,
        block_timestamp
    FROM
        raw
    WHERE
        function_name = 'matchOrders'
),
{% if not is_incremental() %}
    match_orders_mints_raw AS (
        SELECT
            tx_hash,
            function_name,
            trace_index,
            event_type,
            intra_grouping,
            currency_address,
            nft_address,
            tokenid,
            erc1155_value,
            seller_address,
            buyer_address
        FROM
            match_orders_raw
        WHERE
            block_timestamp :: DATE BETWEEN '2021-11-20'
            AND '2022-08-18'
            AND nft_address IS NULL
    ),
    nft_transfers_old AS (
        SELECT
            *
        FROM
            {{ ref('silver__nft_transfers') }}
        WHERE
            block_timestamp :: DATE BETWEEN '2021-06-01'
            AND '2023-02-09'
    ),
    nft_transfers_match_orders_mints_raw AS (
        SELECT
            tx_hash,
            event_index,
            contract_address,
            tokenid,
            erc1155_value,
            ROW_NUMBER() over (
                PARTITION BY tx_hash
                ORDER BY
                    event_index ASC
            ) AS intra_grouping_transfers,
        FROM
            nft_transfers_old
        WHERE
            block_timestamp :: DATE BETWEEN '2021-11-20'
            AND '2022-08-18'
            AND tx_hash IN (
                SELECT
                    tx_hash
                FROM
                    match_orders_mints_raw
                WHERE
                    nft_address IS NULL
            )
            AND from_address = '0x0000000000000000000000000000000000000000'
    ),
    nft_transfers_match_orders_mints AS (
        SELECT
            *,
            LAST_VALUE(intra_grouping_transfers) over (
                PARTITION BY tx_hash
                ORDER BY
                    intra_grouping_transfers ASC
            ) AS mint_count
        FROM
            nft_transfers_match_orders_mints_raw
    ),
    match_orders_mints AS (
        SELECT
            tx_hash,
            m.function_name,
            m.trace_index,
            m.event_type,
            n.intra_grouping_transfers AS intra_grouping,
            m.currency_address,
            n.contract_address AS nft_address,
            n.tokenid,
            n.erc1155_value,
            m.seller_address,
            m.buyer_address,
            mint_count
        FROM
            match_orders_mints_raw m
            INNER JOIN nft_transfers_match_orders_mints n USING (tx_hash)
    ),
    nft_transfers_null_buyer AS (
        SELECT
            tx_hash,
            to_address AS nft_to_address
        FROM
            nft_transfers_old
        WHERE
            block_timestamp :: DATE BETWEEN '2021-09-14'
            AND '2023-02-09'
            AND tx_hash IN (
                SELECT
                    tx_hash
                FROM
                    match_orders_raw
                WHERE
                    buyer_address = '0x0000000000000000000000000000000000000000'
                    AND block_timestamp :: DATE BETWEEN '2021-09-14'
                    AND '2023-02-09'
            )
            AND from_address != '0x0000000000000000000000000000000000000000'
    ),
    match_order_null_buyer AS (
        SELECT
            tx_hash,
            function_name,
            trace_index,
            event_type,
            intra_grouping,
            currency_address,
            nft_address,
            tokenid,
            erc1155_value,
            seller_address,
            n.nft_to_address AS buyer_address
        FROM
            match_orders_raw m
            LEFT JOIN nft_transfers_null_buyer n USING (tx_hash)
        WHERE
            m.buyer_address = '0x0000000000000000000000000000000000000000'
            AND m.block_timestamp :: DATE BETWEEN '2021-09-14'
            AND '2023-02-09'
    ),
    unverified_traces_raw AS (
        SELECT
            tx_hash,
            block_timestamp,
            trace_index,
            from_address AS origin_sender,
            to_address,
        FROM
            {{ ref('core__fact_traces') }}
        WHERE
            block_timestamp :: DATE >= '2021-06-01'
            AND block_timestamp <= '2021-07-28 14:41:59.000'
            AND tx_hash NOT IN (
                SELECT
                    tx_hash
                FROM
                    raw
                WHERE
                    block_timestamp :: DATE >= '2021-06-01'
                    AND block_timestamp <= '2021-07-28 14:41:59.000'
            )
            AND LEFT(
                input,
                10
            ) = '0xe99a3f80'
            AND tx_succeeded
            AND trace_succeeded
            AND to_address = '0x9757f2d2b135150bbeb65308d4a91804107cd8d6'
    ),
    unverified_nft_transfers AS (
        SELECT
            tx_hash,
            event_index,
            from_address AS seller_address,
            to_address AS buyer_address,
            contract_address AS nft_address,
            tokenid,
            erc1155_value,
        FROM
            nft_transfers_old
        WHERE
            block_timestamp :: DATE >= '2021-06-01'
            AND block_timestamp <= '2021-07-28 14:41:59.000'
            AND tx_hash IN (
                SELECT
                    tx_hash
                FROM
                    unverified_traces_raw
            )
    ),
    unverified_traces AS (
        SELECT
            *,
            1 AS intra_grouping,
            IFF(
                origin_sender = buyer_address,
                'sale',
                'bid_won'
            ) AS event_type
        FROM
            unverified_nft_transfers
            INNER JOIN unverified_traces_raw USING (tx_hash) qualify ROW_NUMBER() over (
                PARTITION BY tx_hash
                ORDER BY
                    event_index ASC
            ) = 1
    ),
{% endif %}

traces_final AS (
    SELECT
        tx_hash,
        trace_index,
        function_name,
        'direct_purchase' AS TYPE,
        event_type,
        buyer_address,
        seller_address,
        nft_standard_raw,
        nft_address,
        tokenid,
        erc1155_value,
        currency_address,
        intra_grouping,
        1 AS mint_count
    FROM
        direct_purchase_raw
    UNION ALL
    SELECT
        tx_hash,
        trace_index,
        function_name,
        'direct_accept_bid' AS TYPE,
        event_type,
        buyer_address,
        seller_address,
        nft_standard_raw,
        nft_address,
        tokenid,
        erc1155_value,
        currency_address,
        intra_grouping,
        1 AS mint_count
    FROM
        direct_accept_bid_raw
    UNION ALL
    SELECT
        tx_hash,
        trace_index,
        function_name,
        'match_orders' AS TYPE,
        event_type,
        buyer_address,
        seller_address,
        'match' AS nft_standard_raw,
        nft_address,
        tokenid,
        erc1155_value,
        currency_address,
        intra_grouping,
        1 AS mint_count
    FROM
        match_orders_raw
    WHERE
        nft_address IS NOT NULL
        AND buyer_address != '0x0000000000000000000000000000000000000000' {% if not is_incremental() %}
        UNION ALL
        SELECT
            tx_hash,
            trace_index,
            'matchOrders_unverified' AS function_name,
            'unverified_contract' AS TYPE,
            event_type,
            buyer_address,
            seller_address,
            'unverified' AS nft_standard_raw,
            nft_address,
            tokenid,
            erc1155_value,
            NULL AS currency_address,
            intra_grouping,
            1 AS mint_count
        FROM
            unverified_traces
        UNION ALL
        SELECT
            tx_hash,
            trace_index,
            function_name,
            'match_orders_mint' AS TYPE,
            event_type,
            buyer_address,
            seller_address,
            'match_mint' AS nft_standard_raw,
            nft_address,
            tokenid,
            erc1155_value,
            currency_address,
            intra_grouping,
            mint_count
        FROM
            match_orders_mints
        UNION ALL
        SELECT
            tx_hash,
            trace_index,
            function_name,
            'match_orders_null_buyer' AS TYPE,
            event_type,
            buyer_address,
            seller_address,
            'match_null_buyer' AS nft_standard_raw,
            nft_address,
            tokenid,
            erc1155_value,
            currency_address,
            intra_grouping,
            1 AS mint_count
        FROM
            match_order_null_buyer
        {% endif %}
),
payment_transfers AS (
    SELECT
        tx_hash,
        trace_index,
        from_address,
        to_address,
        VALUE AS eth_value,
        regexp_substr_all(SUBSTR(input, 11, len(input)), '.{64}') AS segmented_input,
        LEFT(
            input,
            10
        ) AS function_sig,
        CASE
            WHEN function_sig = '0x776062c3' THEN '0x' || SUBSTR(
                segmented_input [0] :: STRING,
                25
            )
            WHEN function_sig = '0x' THEN 'ETH'
            ELSE NULL
        END AS payment_currency_address,
        CASE
            WHEN function_sig = '0x776062c3' THEN '0x' || SUBSTR(
                segmented_input [2] :: STRING,
                25
            )
            WHEN function_sig = '0x' THEN to_address
            ELSE NULL
        END AS payment_to_address,
        CASE
            WHEN function_sig = '0x776062c3' THEN utils.udf_hex_to_int(SUBSTR(segmented_input [3] :: STRING, 25)) :: INT
            WHEN function_sig = '0x' THEN eth_value * pow(
                10,
                18
            )
            ELSE NULL
        END AS amount_transferred,
        IFF(
            function_sig IN (
                '0xe99a3f80',
                '0x67d49a3b',
                '0x0d5f7d35'
            )
            AND to_address = '0x9757f2d2b135150bbeb65308d4a91804107cd8d6',
            ROW_NUMBER() over (
                PARTITION BY tx_hash
                ORDER BY
                    trace_index ASC
            ),
            NULL
        ) AS intra_grouping_raw
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        block_timestamp :: DATE >= '2021-06-01'
        AND (
            to_address = '0x9757f2d2b135150bbeb65308d4a91804107cd8d6'
            OR (
                from_address = '0x9757f2d2b135150bbeb65308d4a91804107cd8d6'
                AND to_address = '0xb8e4526e0da700e9ef1f879af713d691f81507d8'
                AND eth_value = 0
            )
            OR (
                from_address = '0x9757f2d2b135150bbeb65308d4a91804107cd8d6'
                AND to_address != '0xb8e4526e0da700e9ef1f879af713d691f81507d8'
                AND eth_value > 0
            )
        )
        AND trace_succeeded
        AND tx_succeeded
        AND function_sig IN (
            '0x776062c3',
            -- erc20 transfer
            '0x',
            -- transfers
            '0xe99a3f80',
            -- match
            '0x67d49a3b',
            -- direct accept bid
            '0x0d5f7d35' -- direct purchase
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
payment_transfers_fill AS (
    SELECT
        *,
        IFF(
            function_sig IN (
                '0xe99a3f80',
                '0x67d49a3b',
                '0x0d5f7d35'
            ),
            intra_grouping_raw,
            LAG(intra_grouping_raw) ignore nulls over (
                PARTITION BY tx_hash
                ORDER BY
                    trace_index ASC
            )
        ) AS intra_grouping_fill
    FROM
        payment_transfers
),
payment_transfers_currency_fill AS (
    SELECT
        *,
        IFF(
            payment_currency_address IS NULL,
            LEAD(payment_currency_address) over (
                PARTITION BY tx_hash,
                intra_grouping_fill
                ORDER BY
                    trace_index ASC
            ),
            payment_currency_address
        ) AS currency_address_fill
    FROM
        payment_transfers_fill
),
payment_transfers_agg_raw AS (
    SELECT
        *,
        IFF(
            payment_to_address = '0x1cf0df2a5a20cd61d68d4489eebbf85b8d39e18a',
            amount_transferred,
            0
        ) AS platform_fee,
        IFF(
            amount_transferred IS NOT NULL
            AND ROW_NUMBER() over (
                PARTITION BY tx_hash,
                currency_address_fill,
                intra_grouping_fill
                ORDER BY
                    amount_transferred DESC
            ) = 1,
            amount_transferred,
            0
        ) AS sale_price,
        IFF(
            amount_transferred IS NOT NULL
            AND platform_fee = 0
            AND sale_price = 0,
            amount_transferred,
            0
        ) AS creator_fee,
        IFF(
            sale_price > 0,
            payment_to_address,
            NULL
        ) AS seller_address_payment
    FROM
        payment_transfers_currency_fill
    WHERE
        amount_transferred IS NOT NULL
),
payment_transfers_agg AS (
    SELECT
        tx_hash,
        currency_address_fill AS currency_address,
        intra_grouping_fill,
        SUM(platform_fee) AS platform_fee_raw,
        SUM(creator_fee) AS creator_fee_raw,
        SUM(sale_price) AS sale_price_raw
    FROM
        payment_transfers_agg_raw
    GROUP BY
        ALL
),
payment_seller_address AS (
    SELECT
        tx_hash,
        seller_address_payment,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                intra_grouping_fill ASC
        ) AS intra_grouping
    FROM
        payment_transfers_agg_raw
    WHERE
        seller_address_payment IS NOT NULL
),
payment_transfers_final AS (
    SELECT
        tx_hash,
        currency_address,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                intra_grouping_fill ASC
        ) AS intra_grouping,
        platform_fee_raw,
        creator_fee_raw,
        sale_price_raw
    FROM
        payment_transfers_agg
),
payment_final AS (
    SELECT
        tx_hash,
        intra_grouping,
        currency_address,
        seller_address_payment,
        platform_fee_raw,
        creator_fee_raw,
        sale_price_raw
    FROM
        payment_transfers_final
        LEFT JOIN payment_seller_address USING (
            tx_hash,
            intra_grouping
        )
),
logs AS (
    SELECT
        tx_hash,
        event_index,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS intra_grouping,
        block_number,
        block_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2021-06-01'
        AND contract_address = '0x9757f2d2b135150bbeb65308d4a91804107cd8d6'
        AND topics [0] :: STRING IN (
            '0x268820db288a211986b26a8fda86b1e0046281b21206936bb0e61c67b5c79ef4',
            '0x956cd63ee4cdcd81fda5f0ec7c6c36dceda99e1b412f4a650a5d26055dc3c450'
        )
        AND tx_succeeded

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
        block_timestamp :: DATE >= '2021-06-01'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                logs
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
final_base AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        intra_grouping,
        '0x9757f2d2b135150bbeb65308d4a91804107cd8d6' AS platform_address,
        'rarible' AS platform_name,
        'rarible v2' AS platform_exchange_version,
        event_index,
        trace_index,
        function_name,
        TYPE,
        event_type,
        nft_standard_raw,
        buyer_address,
        IFF(
            seller_address = '0x0000000000000000000000000000000000000000',
            seller_address_payment,
            seller_address
        ) AS seller_address,
        nft_address,
        tokenid,
        erc1155_value,
        f.currency_address AS traces_currency_address,
        p.currency_address AS payment_currency_address,
        p.currency_address AS currency_address,
        platform_fee_raw / mint_count AS platform_fee_raw_,
        creator_fee_raw / mint_count AS creator_fee_raw_,
        sale_price_raw / mint_count AS sale_price_raw_,
        platform_fee_raw_ + creator_fee_raw_ + sale_price_raw_ AS total_price_raw,
        platform_fee_raw_ + creator_fee_raw_ AS total_fees_raw,
        t.tx_fee,
        t.from_address AS origin_from_address,
        t.to_address AS origin_to_address,
        t.origin_function_signature,
        input_data,
        _log_id,
        CONCAT(
            nft_address,
            '-',
            tokenid,
            '-',
            platform_exchange_version,
            '-',
            _log_id
        ) AS nft_log_id,
        _inserted_timestamp
    FROM
        logs l
        INNER JOIN traces_final f USING (
            tx_hash,
            intra_grouping
        )
        INNER JOIN payment_final p USING (
            tx_hash,
            intra_grouping
        )
        INNER JOIN tx_data t USING (tx_hash)
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    intra_grouping,
    platform_address,
    platform_name,
    platform_exchange_version,
    event_index,
    trace_index,
    function_name,
    TYPE,
    event_type,
    nft_standard_raw,
    buyer_address,
    seller_address,
    nft_address,
    tokenid,
    erc1155_value,
    traces_currency_address,
    payment_currency_address,
    currency_address,
    platform_fee_raw_ AS platform_fee_raw,
    creator_fee_raw_ AS creator_fee_raw,
    sale_price_raw_ AS sale_price_raw,
    total_price_raw,
    total_fees_raw,
    tx_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    input_data,
    _log_id,
    nft_log_id,
    _inserted_timestamp
FROM
    final_base
