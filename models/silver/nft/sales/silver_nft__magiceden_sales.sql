{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH trace_raw AS (

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
        tx_hash
    FROM
        trace_raw
    WHERE
        to_address = '0x5ebc127fae83ed5bdd91fc6a5f5767e259df5642'
        AND function_name = 'forwardCall'
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
        creator_fee_percent * total_price_raw AS creator_fee_raw,
        decoded_input :saleDetails :paymentMethod :: STRING AS currency_address_raw,
        decoded_input :saleDetails :protocol :: INT AS protocol_id,
        decoded_input :saleDetails :tokenAddress :: STRING AS nft_address,
        decoded_input :saleDetails :tokenId :: STRING AS tokenId,
        decoded_input :saleDetails :maker :: STRING AS seller_address_raw,
        decoded_input :cosignature :taker :: STRING AS buyer_address,
        decoded_input :feeOnTop :amount :: INT AS extra_fee_amount,
        decoded_input :feeOnTop :recipient :: STRING AS extra_fee_recipient
    FROM
        trace_raw
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
        creator_fee_percent * total_price_raw AS creator_fee_raw,
        decoded_input :saleDetails :paymentMethod :: STRING AS currency_address_raw,
        decoded_input :saleDetails :protocol :: INT AS protocol_id,
        decoded_input :saleDetails :tokenAddress :: STRING AS nft_address,
        decoded_input :saleDetails :tokenId :: STRING AS tokenId,
        decoded_input :saleDetails :maker :: STRING AS buyer_address,
        decoded_input :cosignature :taker :: STRING AS seller_address_raw,
        -- need to combine with events to get seller address
        decoded_input :feeOnTop :amount :: INT AS extra_fee_amount,
        decoded_input :feeOnTop :recipient :: STRING AS extra_fee_recipient
    FROM
        trace_raw
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
        creator_fee_percent * total_price_raw AS creator_fee_raw,
        VALUE :paymentMethod :: STRING AS currency_address_raw,
        VALUE :protocol :: INT AS protocol_id,
        VALUE :tokenAddress :: STRING AS nft_address,
        VALUE :tokenId :: STRING AS tokenId,
        VALUE :maker :: STRING AS seller_address_raw
    FROM
        trace_raw,
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
        trace_raw,
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
        creator_fee_raw,
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
        creator_fee_percent * total_price_raw AS creator_fee_raw,
        VALUE :paymentMethod :: STRING AS currency_address_raw,
        VALUE :protocol :: INT AS protocol_id,
        VALUE :tokenAddress :: STRING AS nft_address,
        VALUE :tokenId :: STRING AS tokenId,
        VALUE :maker :: STRING AS seller_address_raw -- value:cosignature:taker::string as buyer_address,
        -- value:feeOnTop:amount::int as extra_fee_amount,
        -- value:feeOnTop:recipient::string as extra_fee_recipient
    FROM
        trace_raw,
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
        trace_raw,
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
        creator_fee_raw,
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
        creator_fee_percent * total_price_raw AS creator_fee_raw,
        VALUE :tokenId :: STRING AS tokenId,
        decoded_input :feeOnTop :amount :: INT AS extra_fee_amount,
        decoded_input :feeOnTop :recipient :: STRING AS extra_fee_recipient
    FROM
        trace_raw,
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
        creator_fee_raw,
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
        creator_fee_raw,
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
        creator_fee_raw,
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
        creator_fee_raw,
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
        creator_fee_raw,
        currency_address_raw,
        protocol_id,
        nft_address,
        tokenId,
        extra_fee_amount,
        extra_fee_recipient
    FROM
        sweep_collection_raw
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
        creator_fee_raw,
        currency_address_raw,
        protocol_id,
        nft_address,
        tokenId,
        extra_fee_amount,
        extra_fee_recipient
    FROM
        all_sales_base
),
nft_transfers AS (
    SELECT
        contract_address AS nft_address,
        token_transfer_type
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        block_timestamp :: DATE >= '2024-02-04'
        AND tx_hash IN (
            SELECT
                tx_hash
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

qualify ROW_NUMBER() over (
    PARTITION BY contract_address
    ORDER BY
        event_index ASC
) = 1
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
)
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
    '0x5ebc127fae83ed5bdd91fc6a5f5767e259df5642' AS platform_address,
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
    token_transfer_type,
    quantity,
    IFF(
        token_transfer_type = 'erc721_Transfer',
        NULL,
        quantity
    ) AS erc1155_value,
    IFF(
        currency_address_raw = '0x0000000000000000000000000000000000000000',
        'ETH',
        currency_address_raw
    ) AS currency_address,
    total_price_raw,
    platform_fee_raw,
    creator_fee_raw,
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
    INNER JOIN nft_transfers USING (nft_address)
    INNER JOIN magiceden_tx_filter USING (tx_hash)
    INNER JOIN tx_data USING (tx_hash)
