{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH decoded_trace AS (

    SELECT
        tx_hash,
        trace_index,
        from_address,
        to_address,
        LEFT(
            input,
            10
        ) AS function_sig,
        decoded_data :function_name :: STRING AS function_name,
        decoded_data :decoded_input_data AS decoded_input,
        input,
        regexp_substr_all(SUBSTR(input, 11, len(input)), '.{64}') AS segmented_input
    FROM
        {{ ref('silver__decoded_traces') }}
    WHERE
        block_timestamp :: DATE >= '2024-02-04'
        AND to_address IN (
            '0x5ebc127fae83ed5bdd91fc6a5f5767e259df5642',
            -- magic eden forwarder
            '0xb233e3602bb06aa2c2db0982bbaf33c2b15184c9',
            -- other magic eden forwarder
            '0x9a1d0059f5534e7a6c6c4dae390ebd3a731bd7dc',
            -- ModuleTrades,
            '0x9a1d00899099d06fe50fb31f03db5345c45abb36' -- ModuleTradesAdvanced
        )
        AND function_name IN (
            'forwardCall',
            'bulkAcceptOffers',
            'bulkBuyListings',
            'buyListing',
            'acceptOffer',
            'sweepCollection'
        )
        AND trace_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '24 hours'
    FROM
        {{ this }}
)
{% endif %}
),
magiceden_tx_filter AS (
    SELECT
        DISTINCT tx_hash,
        to_address AS platform_address
    FROM
        decoded_trace
    WHERE
        to_address IN (
            '0x5ebc127fae83ed5bdd91fc6a5f5767e259df5642',
            '0xb233e3602bb06aa2c2db0982bbaf33c2b15184c9'
        )
        AND function_name = 'forwardCall'
),
raw_traces AS (
    SELECT
        tx_hash,
        trace_index,
        LEFT(
            input,
            10
        ) AS function_sig,
        from_address,
        to_address,
        eth_value,
        input,
        output,
        regexp_substr_all(SUBSTR(input, 11, len(input)), '.{64}') AS segmented_input,
        regexp_substr_all(SUBSTR(output, 3, len(output)), '.{64}') AS segmented_output,
        trace_status,
        TYPE
    FROM
        {{ ref('silver__traces') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                magiceden_tx_filter
        )
        AND block_timestamp :: DATE >= '2024-02-06'
        AND LEFT(
            input,
            10
        ) IN (
            '0x2a55205a',
            -- royaltyInfo
            '0x23b872dd',
            -- transferFrom
            '0x'
        )
        AND from_address = LOWER('0x9A1D00bEd7CD04BCDA516d721A596eb22Aac6834') -- payment processor

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '24 hours'
    FROM
        {{ this }}
)
{% endif %}
),
royalty_trace AS (
    SELECT
        tx_hash,
        segmented_input,
        segmented_output,
        utils.udf_hex_to_int(
            segmented_input [1]
        ) :: INT AS trace_sale_price,
        utils.udf_hex_to_int(
            segmented_input [0]
        ) :: STRING AS trace_tokenId,
        IFF(
            trace_status = 'FAIL',
            '0x0000000000000000000000000000000000000000',
            '0x' || SUBSTR(
                segmented_output [0] :: STRING,
                25
            )
        ) AS trace_royalty_receiver,
        IFF(
            trace_status = 'FAIL',
            0,
            utils.udf_hex_to_int(
                segmented_output [1]
            ) :: INT
        ) AS trace_royalty_amount,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) AS intra_tx_grouping
    FROM
        raw_traces
    WHERE
        function_sig = '0x2a55205a' -- royaltyInfo
),
raw_events AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        event_name,
        decoded_flat :seller :: STRING AS seller_address,
        decoded_flat :buyer :: STRING AS buyer_address,
        decoded_flat :tokenAddress :: STRING AS event_nft_address,
        decoded_flat :tokenId :: STRING AS event_tokenid,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS intra_tx_grouping,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        contract_address = '0x9a1d00bed7cd04bcda516d721a596eb22aac6834' -- payment processor
        AND block_timestamp :: DATE >= '2024-02-04'
        AND event_name IN (
            'BuyListingERC1155',
            'BuyListingERC721',
            'AcceptOfferERC1155',
            'AcceptOfferERC721'
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
buy_listing_raw AS (
    SELECT
        tx_hash,
        trace_index,
        1 AS INDEX,
        function_name,
        decoded_input :saleDetails :amount :: STRING AS quantity,
        decoded_input :saleDetails :itemPrice :: INT AS total_price_raw,
        decoded_input :saleDetails :marketplace :: STRING AS marketplace_address,
        decoded_input :saleDetails :marketplaceFeeNumerator / pow(
            10,
            4
        ) AS platform_fee_percent,
        platform_fee_percent * total_price_raw AS platform_fee_raw,
        decoded_input :saleDetails :maxRoyaltyFeeNumerator / pow(
            10,
            4
        ) AS creator_fee_percent,
        decoded_input :saleDetails :fallbackRoyaltyRecipient :: STRING AS fallback_royalty_recipient,
        creator_fee_percent * total_price_raw AS fallback_creator_fee_raw,
        decoded_input :saleDetails :paymentMethod :: STRING AS currency_address_raw,
        decoded_input :saleDetails :protocol :: INT AS protocol_id,
        decoded_input :saleDetails :tokenAddress :: STRING AS nft_address,
        decoded_input :saleDetails :tokenId :: STRING AS tokenId,
        decoded_input :saleDetails :maker :: STRING AS seller_address_raw,
        decoded_input :cosignature :taker :: STRING AS buyer_address,
        decoded_input :feeOnTop :amount :: INT AS extra_fee_amount,
        decoded_input :feeOnTop :recipient :: STRING AS extra_fee_recipient
    FROM
        decoded_trace
    WHERE
        function_name = 'buyListing'
),
accept_offer_raw AS (
    SELECT
        tx_hash,
        trace_index,
        1 AS INDEX,
        function_name,
        decoded_input :saleDetails :amount :: STRING AS quantity,
        decoded_input :saleDetails :itemPrice :: INT AS total_price_raw,
        decoded_input :saleDetails :marketplace :: STRING AS marketplace_address,
        decoded_input :saleDetails :marketplaceFeeNumerator / pow(
            10,
            4
        ) AS platform_fee_percent,
        platform_fee_percent * total_price_raw AS platform_fee_raw,
        decoded_input :saleDetails :maxRoyaltyFeeNumerator / pow(
            10,
            4
        ) AS creator_fee_percent,
        decoded_input :saleDetails :fallbackRoyaltyRecipient :: STRING AS fallback_royalty_recipient,
        creator_fee_percent * total_price_raw AS fallback_creator_fee_raw,
        decoded_input :saleDetails :paymentMethod :: STRING AS currency_address_raw,
        decoded_input :saleDetails :protocol :: INT AS protocol_id,
        decoded_input :saleDetails :tokenAddress :: STRING AS nft_address,
        decoded_input :saleDetails :tokenId :: STRING AS tokenId,
        decoded_input :saleDetails :maker :: STRING AS buyer_address,
        decoded_input :cosignature :taker :: STRING AS seller_address_raw,
        decoded_input :feeOnTop :amount :: INT AS extra_fee_amount,
        decoded_input :feeOnTop :recipient :: STRING AS extra_fee_recipient
    FROM
        decoded_trace
    WHERE
        function_name = 'acceptOffer'
),
bulk_buy_listing_sale_details_raw AS (
    SELECT
        tx_hash,
        trace_index,
        INDEX,
        function_name,
        VALUE :amount :: STRING AS quantity,
        VALUE :itemPrice :: INT AS total_price_raw,
        VALUE :marketplace :: STRING AS marketplace_address,
        VALUE :marketplaceFeeNumerator / pow(
            10,
            4
        ) AS platform_fee_percent,
        platform_fee_percent * total_price_raw AS platform_fee_raw,
        VALUE :maxRoyaltyFeeNumerator / pow(
            10,
            4
        ) AS creator_fee_percent,
        VALUE :fallbackRoyaltyRecipient :: STRING AS fallback_royalty_recipient,
        creator_fee_percent * total_price_raw AS fallback_creator_fee_raw,
        VALUE :paymentMethod :: STRING AS currency_address_raw,
        VALUE :protocol :: INT AS protocol_id,
        VALUE :tokenAddress :: STRING AS nft_address,
        VALUE :tokenId :: STRING AS tokenId,
        VALUE :maker :: STRING AS seller_address_raw
    FROM
        decoded_trace,
        LATERAL FLATTEN(
            input => decoded_input :saleDetailsArray
        )
    WHERE
        function_name = 'bulkBuyListings'
),
bulk_buy_listing_extra_fees AS (
    SELECT
        tx_hash,
        trace_index,
        INDEX,
        VALUE :amount :: INT AS extra_fee_amount,
        VALUE :recipient :: STRING AS extra_fee_recipient
    FROM
        decoded_trace,
        LATERAL FLATTEN(
            input => decoded_input :feesOnTop
        )
    WHERE
        function_name = 'bulkBuyListings'
),
bulk_buy_listing_base AS (
    SELECT
        tx_hash,
        trace_index,
        INDEX,
        function_name,
        quantity,
        total_price_raw,
        marketplace_address,
        platform_fee_percent,
        platform_fee_raw,
        creator_fee_percent,
        fallback_royalty_recipient,
        fallback_creator_fee_raw,
        currency_address_raw,
        protocol_id,
        nft_address,
        tokenId,
        seller_address_raw,
        extra_fee_amount,
        extra_fee_recipient
    FROM
        bulk_buy_listing_sale_details_raw
        LEFT JOIN bulk_buy_listing_extra_fees USING (
            tx_hash,
            trace_index,
            INDEX
        )
),
bulk_accept_offer_sale_details_raw AS (
    SELECT
        tx_hash,
        trace_index,
        INDEX,
        function_name,
        VALUE :amount :: STRING AS quantity,
        VALUE :itemPrice :: INT AS total_price_raw,
        VALUE :marketplace :: STRING AS marketplace_address,
        VALUE :marketplaceFeeNumerator / pow(
            10,
            4
        ) AS platform_fee_percent,
        platform_fee_percent * total_price_raw AS platform_fee_raw,
        VALUE :maxRoyaltyFeeNumerator / pow(
            10,
            4
        ) AS creator_fee_percent,
        VALUE :fallbackRoyaltyRecipient :: STRING AS fallback_royalty_recipient,
        creator_fee_percent * total_price_raw AS fallback_creator_fee_raw,
        VALUE :paymentMethod :: STRING AS currency_address_raw,
        VALUE :protocol :: INT AS protocol_id,
        VALUE :tokenAddress :: STRING AS nft_address,
        VALUE :tokenId :: STRING AS tokenId,
        VALUE :maker :: STRING AS seller_address_raw
    FROM
        decoded_trace,
        LATERAL FLATTEN(
            input => decoded_input :params :saleDetailsArray
        )
    WHERE
        function_name = 'bulkAcceptOffers'
),
bulk_accept_offer_extra_fees AS (
    SELECT
        tx_hash,
        trace_index,
        INDEX,
        VALUE,
        VALUE :amount :: INT AS extra_fee_amount,
        VALUE :recipient :: STRING AS extra_fee_recipient
    FROM
        decoded_trace,
        LATERAL FLATTEN(
            input => decoded_input :params :feesOnTopArray
        )
    WHERE
        function_name = 'bulkAcceptOffers'
),
bulk_accept_offer_base AS (
    SELECT
        tx_hash,
        trace_index,
        INDEX,
        function_name,
        quantity,
        total_price_raw,
        marketplace_address,
        platform_fee_percent,
        platform_fee_raw,
        creator_fee_percent,
        fallback_royalty_recipient,
        fallback_creator_fee_raw,
        currency_address_raw,
        protocol_id,
        nft_address,
        tokenId,
        seller_address_raw,
        extra_fee_amount,
        extra_fee_recipient
    FROM
        bulk_accept_offer_sale_details_raw
        LEFT JOIN bulk_accept_offer_extra_fees USING (
            tx_hash,
            trace_index,
            INDEX
        )
),
sweep_collection_raw AS (
    SELECT
        tx_hash,
        trace_index,
        INDEX,
        function_name,
        decoded_input,
        decoded_input :sweepOrder :paymentMethod :: STRING AS currency_address_raw,
        decoded_input :sweepOrder :protocol :: INT AS protocol_id,
        decoded_input :sweepOrder :tokenAddress :: STRING AS nft_address,
        VALUE :amount :: STRING AS quantity,
        VALUE :itemPrice :: INT AS total_price_raw,
        VALUE :marketplace :: STRING AS marketplace_address,
        VALUE :marketplaceFeeNumerator / pow(
            10,
            4
        ) AS platform_fee_percent,
        platform_fee_percent * total_price_raw AS platform_fee_raw,
        VALUE :maxRoyaltyFeeNumerator / pow(
            10,
            4
        ) AS creator_fee_percent,
        VALUE :fallbackRoyaltyRecipient :: STRING AS fallback_royalty_recipient,
        creator_fee_percent * total_price_raw AS fallback_creator_fee_raw,
        VALUE :tokenId :: STRING AS tokenId,
        decoded_input :feeOnTop :amount :: INT AS extra_fee_amount,
        decoded_input :feeOnTop :recipient :: STRING AS extra_fee_recipient
    FROM
        decoded_trace,
        LATERAL FLATTEN(
            input => decoded_input :items
        )
    WHERE
        function_name = 'sweepCollection'
),
all_sales_base AS (
    SELECT
        tx_hash,
        trace_index,
        INDEX,
        function_name,
        quantity,
        total_price_raw,
        marketplace_address,
        platform_fee_percent,
        platform_fee_raw,
        creator_fee_percent,
        fallback_royalty_recipient,
        fallback_creator_fee_raw,
        currency_address_raw,
        protocol_id,
        nft_address,
        tokenId,
        extra_fee_amount,
        extra_fee_recipient
    FROM
        buy_listing_raw
    UNION ALL
    SELECT
        tx_hash,
        trace_index,
        INDEX,
        function_name,
        quantity,
        total_price_raw,
        marketplace_address,
        platform_fee_percent,
        platform_fee_raw,
        creator_fee_percent,
        fallback_royalty_recipient,
        fallback_creator_fee_raw,
        currency_address_raw,
        protocol_id,
        nft_address,
        tokenId,
        extra_fee_amount,
        extra_fee_recipient
    FROM
        accept_offer_raw
    UNION ALL
    SELECT
        tx_hash,
        trace_index,
        INDEX,
        function_name,
        quantity,
        total_price_raw,
        marketplace_address,
        platform_fee_percent,
        platform_fee_raw,
        creator_fee_percent,
        fallback_royalty_recipient,
        fallback_creator_fee_raw,
        currency_address_raw,
        protocol_id,
        nft_address,
        tokenId,
        extra_fee_amount,
        extra_fee_recipient
    FROM
        bulk_buy_listing_base
    UNION ALL
    SELECT
        tx_hash,
        trace_index,
        INDEX,
        function_name,
        quantity,
        total_price_raw,
        marketplace_address,
        platform_fee_percent,
        platform_fee_raw,
        creator_fee_percent,
        fallback_royalty_recipient,
        fallback_creator_fee_raw,
        currency_address_raw,
        protocol_id,
        nft_address,
        tokenId,
        extra_fee_amount,
        extra_fee_recipient
    FROM
        bulk_accept_offer_base
    UNION ALL
    SELECT
        tx_hash,
        trace_index,
        INDEX,
        function_name,
        quantity,
        total_price_raw,
        marketplace_address,
        platform_fee_percent,
        platform_fee_raw,
        creator_fee_percent,
        fallback_royalty_recipient,
        fallback_creator_fee_raw,
        currency_address_raw,
        protocol_id,
        nft_address,
        tokenId,
        extra_fee_amount,
        extra_fee_recipient
    FROM
        sweep_collection_raw
),
royalty_for_tokens_raw AS (
    SELECT
        tx_hash,
        trace_index,
        from_address,
        to_address,
        eth_value,
        input,
        output,
        segmented_input,
        segmented_output,
        TYPE,
        trace_status,
        function_sig,
        IFF(LEFT(input, 10) = '0x2a55205a', ROW_NUMBER() over (PARTITION BY tx_hash
    ORDER BY
        trace_index ASC), NULL) AS grouping_raw
    FROM
        raw_traces
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                all_sales_base
        )
        AND function_sig IN (
            '0x23b872dd',
            -- transferFrom
            '0x2a55205a',
            -- royaltyInfo
            '0x'
        )
),
grouping_fill AS (
    SELECT
        *,
        CASE
            WHEN grouping_raw IS NULL THEN LAG(grouping_raw) ignore nulls over (
                PARTITION BY tx_hash
                ORDER BY
                    trace_index ASC
            )
            ELSE grouping_raw
        END AS grouping_raw_fill
    FROM
        royalty_for_tokens_raw qualify grouping_raw_fill IS NOT NULL
),
token_transfer_tags AS (
    SELECT
        *,
        '0x' || SUBSTR(
            segmented_input [0] :: STRING,
            25
        ) AS token_from_address,
        '0x' || SUBSTR(
            segmented_input [1] :: STRING,
            25
        ) AS token_to_address,
        utils.udf_hex_to_int(
            segmented_input [2]
        ) :: INT AS raw_amount_transferred,
        IFF(
            token_to_address = '0xca9337244b5f04cb946391bc8b8a980e988f9a6a',
            raw_amount_transferred,
            0
        ) AS platform_fees_traces,
        IFF(ROW_NUMBER() over (PARTITION BY tx_hash, grouping_raw_fill
    ORDER BY
        raw_amount_transferred DESC) = 1, raw_amount_transferred, 0) AS sale_amount_received_traces,
        IFF(
            platform_fees_traces = 0
            AND sale_amount_received_traces = 0,
            raw_amount_transferred,
            0
        ) AS creator_fee_traces
    FROM
        grouping_fill
    WHERE
        segmented_output IS NOT NULL
        AND trace_status = 'SUCCESS'
        AND TYPE = 'CALL'
        AND eth_value = 0
        AND function_sig = '0x23b872dd'
),
eth_transfer_tags AS (
    SELECT
        *,
        eth_value * pow(
            10,
            18
        ) AS eth_raw_amount,
        IFF(
            to_address = '0xca9337244b5f04cb946391bc8b8a980e988f9a6a',
            eth_raw_amount,
            0
        ) AS platform_fees_traces,
        IFF(ROW_NUMBER() over (PARTITION BY tx_hash, grouping_raw_fill
    ORDER BY
        eth_raw_amount DESC) = 1, eth_raw_amount, 0) AS sale_amount_received_traces,
        IFF(
            platform_fees_traces = 0
            AND sale_amount_received_traces = 0,
            eth_raw_amount,
            0
        ) AS creator_fee_traces
    FROM
        grouping_fill
    WHERE
        segmented_output IS NULL
        AND trace_status = 'SUCCESS'
        AND TYPE = 'CALL'
        AND eth_value > 0
        AND function_sig IN (
            '0x'
        )
),
token_creator_fee_aggregation AS (
    SELECT
        tx_hash,
        grouping_raw_fill,
        SUM(creator_fee_traces) AS creator_fee_traces_agg
    FROM
        token_transfer_tags
    GROUP BY
        ALL
),
eth_creator_fee_aggregation AS (
    SELECT
        tx_hash,
        grouping_raw_fill,
        SUM(creator_fee_traces) AS creator_fee_traces_agg
    FROM
        eth_transfer_tags
    GROUP BY
        ALL
),
creator_fee_combined AS (
    SELECT
        *
    FROM
        token_creator_fee_aggregation
    UNION ALL
    SELECT
        *
    FROM
        eth_creator_fee_aggregation
),
final_tokens_creator_fee AS (
    SELECT
        tx_hash,
        creator_fee_traces_agg,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                grouping_raw_fill ASC
        ) AS intra_tx_grouping
    FROM
        creator_fee_combined
),
final_sales_base AS (
    SELECT
        tx_hash,
        trace_index,
        INDEX,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC,
                INDEX ASC
        ) AS intra_tx_grouping,
        function_name,
        quantity,
        total_price_raw,
        marketplace_address,
        platform_fee_percent,
        platform_fee_raw,
        creator_fee_percent,
        fallback_royalty_recipient,
        fallback_creator_fee_raw,
        currency_address_raw,
        protocol_id,
        nft_address,
        tokenId,
        extra_fee_amount,
        extra_fee_recipient
    FROM
        all_sales_base
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
        block_timestamp :: DATE >= '2024-02-04'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                final_sales_base
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
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        intra_tx_grouping,
        event_index,
        trace_index,
        INDEX,
        function_name,
        CASE
            WHEN function_name IN (
                'bulkBuyListings',
                'buyListing',
                'sweepCollection'
            ) THEN 'sale'
            ELSE 'bid_won'
        END AS event_type,
        platform_address,
        'magic eden' AS platform_name,
        'magic eden v1' AS platform_exchange_version,
        contract_address,
        event_name,
        protocol_id,
        marketplace_address,
        seller_address,
        buyer_address,
        event_nft_address,
        event_tokenid,
        b.nft_address,
        b.tokenId,
        quantity,
        IFF(
            event_name LIKE '%ERC721',
            NULL,
            quantity
        ) AS erc1155_value,
        IFF(
            currency_address_raw = '0x0000000000000000000000000000000000000000',
            'ETH',
            currency_address_raw
        ) AS currency_address,
        fallback_royalty_recipient,
        fallback_creator_fee_raw,
        trace_royalty_receiver,
        trace_royalty_amount,
        creator_fee_traces_agg,
        CASE
            WHEN event_type = 'bid_won' THEN creator_fee_traces_agg
            WHEN event_type = 'sale'
            AND trace_royalty_receiver != '0x0000000000000000000000000000000000000000' THEN trace_royalty_amount
            WHEN event_type = 'sale'
            AND trace_royalty_receiver = '0x0000000000000000000000000000000000000000'
            AND fallback_royalty_recipient != '0x0000000000000000000000000000000000000000' THEN fallback_creator_fee_raw
            WHEN event_type = 'sale'
            AND trace_royalty_receiver = '0x0000000000000000000000000000000000000000'
            AND fallback_royalty_recipient = '0x0000000000000000000000000000000000000000' THEN 0
        END AS creator_fee_raw,
        total_price_raw,
        platform_fee_raw,
        platform_fee_raw + creator_fee_raw AS total_fees_raw,
        platform_fee_percent,
        creator_fee_percent,
        extra_fee_amount,
        extra_fee_recipient,
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
        raw_events r
        INNER JOIN final_sales_base b USING (
            tx_hash,
            intra_tx_grouping
        )
        INNER JOIN magiceden_tx_filter USING (tx_hash)
        INNER JOIN royalty_trace USING (
            tx_hash,
            intra_tx_grouping
        )
        LEFT JOIN final_tokens_creator_fee USING (
            tx_hash,
            intra_tx_grouping
        )
        INNER JOIN tx_data USING (tx_hash) qualify ROW_NUMBER() over (
            PARTITION BY nft_log_id
            ORDER BY
                _inserted_timestamp DESC
        ) = 1
)
SELECT
    *
FROM
    FINAL
