{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH pools AS (

    SELECT
        function_name AS pair_creation_function,
        pool_address,
        token_address,
        nft_address,
        IFF(
            function_name LIKE 'createPairERC1155%',
            initial_nft_id [0] :: STRING,
            NULL
        ) AS erc1155_tokenid,
        pool_type
    FROM
        {{ ref('silver_nft__sudoswap_v2_pools') }}
),
base AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        trace_index,
        from_address,
        to_address,
        decoded_data,
        decoded_data :function_name :: STRING AS function_name,
        pair_creation_function,
        pool_address,
        token_address,
        nft_address,
        erc1155_tokenid,
        pool_type,
        input,
        output,
        IFF(
            function_name IN (
                'swapTokenForSpecificNFTs',
                'swapNFTsForToken',
                'getBuyNFTQuote',
                'getSellNFTQuote'
            ),
            function_name,
            NULL
        ) AS swap_tag,
        IFF(
            function_name IN (
                'swapTokenForSpecificNFTs',
                'swapNFTsForToken',
                'getBuyNFTQuote',
                'getSellNFTQuote'
            ),
            1,
            0
        ) AS group_tag,
        trace_status
    FROM
        {{ ref('silver__decoded_traces') }}
        INNER JOIN pools
        ON from_address = pool_address
    WHERE
        block_timestamp :: DATE >= '2023-05-01'
        AND to_address IN (
            '0x8949eb1d16072ab43a48c0ef9c7d03580bf000cc',
            -- 4 swap contracts
            '0x4f3742021ed9ee6109bde80814bdad9859cebc5f',
            '0xd57b84f2a3c1f68244d3872e35d190b909b00632',
            '0x3b91af330524d05c8d33102b18fdd765c9a5ff00',
            '0x1fd5876d4a3860eb0159055a3b7cb79fdfff6b67',
            -- 4 bonding curve
            '0xfa056c602ad0c0c4ee4385b3233f2cb06730334a',
            '0xe5d78fec1a7f42d2f3620238c498f088a866fdc5',
            '0xc7fb91b6cd3c67e02ec08013cebb29b1241f3de5',
            '0xbc40d21999b4bf120d330ee3a2de415287f626c9' --royalty engine
        )
        AND LEFT(
            input,
            10
        ) IN (
            '0x6d8b99f7',
            -- swapTokenForSpecificNFTs
            '0xb1d3f1c1',
            -- swapNFTsForToken
            '0x7ca542ac',
            -- getBuyInfo
            '0x097cc63d',
            -- getSellInfo
            '0xf533b802',
            -- getRoyalty
            '0x1afd78c5',
            -- getBuyNFTQuote
            '0x33b24a3a' -- getSellNFTQuote
        )
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= CURRENT_DATE() - INTERVAL '7 day'
{% endif %}
),
base_swap_fill AS (
    SELECT
        *,
        IFF(swap_tag IS NULL, LAG(swap_tag) ignore nulls over (PARTITION BY tx_hash
    ORDER BY
        trace_index ASC), swap_tag) AS swap_tag_fill,
        IFF(group_tag IS NULL, LAG(group_tag) ignore nulls over (PARTITION BY tx_hash
    ORDER BY
        trace_index ASC), group_tag) AS group_tag_fill
    FROM
        base
),
base_swap_filtered AS (
    SELECT
        *,
        SUM(group_tag) over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) AS intra_tx_grouping
    FROM
        base_swap_fill
    WHERE
        swap_tag_fill IN (
            'swapTokenForSpecificNFTs',
            'swapNFTsForToken'
        )
),
all_swaps AS (
    SELECT
        *,
        decoded_data :decoded_input_data :isRouter AS is_router,
        COALESCE(
            decoded_data :decoded_input_data :nftIds,
            decoded_data :decoded_input_data :numNFTs
        ) AS nft_id_or_quantity,
        decoded_data :decoded_input_data :routerCaller :: STRING AS router_caller,
        CASE
            WHEN function_name = 'swapNFTsForToken' THEN decoded_data :decoded_input_data :tokenRecipient :: STRING
            ELSE NULL
        END AS token_recipient,
        CASE
            WHEN function_name = 'swapTokenForSpecificNFTs' THEN decoded_data :decoded_input_data :nftRecipient :: STRING
            ELSE NULL
        END AS nft_recipient,
        CASE
            WHEN function_name = 'swapNFTsForToken' THEN decoded_data :decoded_output_data :outputAmount :: INT
            ELSE decoded_data :decoded_output_data :output_1 :: INT
        END AS output_amount
    FROM
        base_swap_filtered
    WHERE
        function_name IN (
            'swapNFTsForToken',
            'swapTokenForSpecificNFTs'
        )
),
get_royalty AS (
    SELECT
        tx_hash,
        intra_tx_grouping,
        decoded_data :decoded_input_data :tokenAddress :: STRING AS nft_address,
        decoded_data :decoded_input_data :tokenId :: STRING AS tokenid,
        decoded_data :decoded_output_data :output_1 AS royalty_receiver_array,
        decoded_data :decoded_output_data :output_2 AS royalty_amount_array
    FROM
        base_swap_filtered
    WHERE
        function_name = 'getRoyalty'
),
get_royalty_agg AS (
    SELECT
        tx_hash,
        intra_tx_grouping,
        nft_address,
        tokenid,
        SUM(VALUE) AS royalty_fee_raw
    FROM
        get_royalty,
        LATERAL FLATTEN (
            input => royalty_amount_array
        )
    GROUP BY
        ALL
),
get_buysell_info AS (
    SELECT
        tx_hash,
        intra_tx_grouping,
        decoded_data :decoded_input_data :delta :: INT AS delta,
        decoded_data :decoded_input_data :feeMultiplier :: INT AS fee_multiplier,
        decoded_data :decoded_input_data :numItems :: INT AS num_items,
        -- can be number of tokenids for 721s, can be quantity of 1155s
        decoded_data :decoded_input_data :protocolFeeMultiplier :: INT AS protocol_fee_multiplier,
        decoded_data :decoded_input_data :spotPrice :: INT AS spot_price,
        decoded_data :decoded_output_data :error AS error,
        decoded_data :decoded_output_data :newSpotPrice :: INT AS new_spot_price,
        decoded_data :decoded_output_data :protocolFee :: INT AS protocol_fee,
        decoded_data :decoded_output_data :tradeFee :: INT AS trade_fee
    FROM
        base_swap_filtered
    WHERE
        function_name IN (
            'getBuyInfo',
            'getSellInfo'
        )
),
combined_base AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        intra_tx_grouping,
        trace_index,
        from_address,
        to_address,
        function_name,
        pair_creation_function,
        pool_address,
        pool_type,
        token_address,
        nft_address,
        erc1155_tokenid,
        nft_id_or_quantity,
        nft_id_or_quantity [0] :: STRING AS first_nft_id,
        num_items,
        token_recipient,
        nft_recipient,
        output_amount,
        protocol_fee,
        r.nft_address AS royalty_nft_address,
        r.tokenid AS royalty_tokenid,
        r.royalty_fee_raw,
        router_caller,
        swap_tag_fill,
        group_tag_fill,
        delta,
        fee_multiplier,
        protocol_fee_multiplier,
        error,
        new_spot_price,
        trade_fee,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) AS intra_tx_grouping_new
    FROM
        all_swaps
        INNER JOIN get_buysell_info USING (
            tx_hash,
            intra_tx_grouping
        )
        LEFT JOIN get_royalty_agg r USING (
            tx_hash,
            intra_tx_grouping
        )
    WHERE
        trace_status = 'SUCCESS'
),
base_721 AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        intra_tx_grouping,
        trace_index,
        from_address,
        to_address,
        function_name,
        pair_creation_function,
        pool_address,
        pool_type,
        token_address,
        token_address AS currency_address,
        nft_address,
        VALUE AS tokenid,
        NULL AS erc1155_value,
        num_items,
        token_recipient,
        nft_recipient,
        CASE
            WHEN function_name = 'swapNFTsForToken' THEN token_recipient
            ELSE pool_address
        END AS seller_address,
        CASE
            WHEN function_name = 'swapNFTsForToken' THEN pool_address
            ELSE nft_recipient
        END AS buyer_address,
        CASE
            WHEN function_name = 'swapNFTsForToken' THEN (output_amount + COALESCE(protocol_fee, 0) + COALESCE(royalty_fee_raw, 0)) / num_items
            ELSE output_amount / num_items
        END AS total_price_raw,
        COALESCE(
            protocol_fee,
            0
        ) / num_items AS platform_fee_raw,
        COALESCE(
            royalty_fee_raw,
            0
        ) / num_items AS creator_fee_raw,
        platform_fee_raw + creator_fee_raw AS total_fees_raw,
        output_amount,
        protocol_fee,
        royalty_fee_raw,
        royalty_nft_address,
        royalty_tokenid,
        router_caller,
        swap_tag_fill,
        group_tag_fill,
        delta,
        fee_multiplier,
        protocol_fee_multiplier,
        error,
        new_spot_price,
        trade_fee / num_items AS trade_fee,
        intra_tx_grouping_new
    FROM
        combined_base,
        LATERAL FLATTEN (
            input => nft_id_or_quantity
        )
    WHERE
        pair_creation_function LIKE 'createPairERC721%'
),
base_1155 AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        intra_tx_grouping,
        trace_index,
        from_address,
        to_address,
        function_name,
        pair_creation_function,
        pool_address,
        pool_type,
        token_address,
        token_address AS currency_address,
        nft_address,
        erc1155_tokenid AS tokenid,
        nft_id_or_quantity [0] :: STRING AS erc1155_value,
        num_items,
        token_recipient,
        nft_recipient,
        CASE
            WHEN function_name = 'swapNFTsForToken' THEN token_recipient
            ELSE pool_address
        END AS seller_address,
        CASE
            WHEN function_name = 'swapNFTsForToken' THEN pool_address
            ELSE nft_recipient
        END AS buyer_address,
        CASE
            WHEN function_name = 'swapNFTsForToken' THEN (output_amount + COALESCE(protocol_fee, 0) + COALESCE(royalty_fee_raw, 0))
            ELSE output_amount
        END AS total_price_raw,
        COALESCE(
            protocol_fee,
            0
        ) AS platform_fee_raw,
        COALESCE(
            royalty_fee_raw,
            0
        ) AS creator_fee_raw,
        platform_fee_raw + creator_fee_raw AS total_fees_raw,
        output_amount,
        protocol_fee,
        royalty_fee_raw,
        royalty_nft_address,
        royalty_tokenid,
        router_caller,
        swap_tag_fill,
        group_tag_fill,
        delta,
        fee_multiplier,
        protocol_fee_multiplier,
        error,
        new_spot_price,
        trade_fee,
        intra_tx_grouping_new
    FROM
        combined_base
    WHERE
        pair_creation_function LIKE 'createPairERC1155%'
),
final_base AS (
    SELECT
        *
    FROM
        base_1155
    UNION ALL
    SELECT
        *
    FROM
        base_721
),
raw_logs AS (
    SELECT
        tx_hash,
        event_index,
        _log_id,
        _inserted_timestamp,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS intra_tx_grouping_new
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address IN (
            SELECT
                pool_address
            FROM
                pools
        )
        AND block_timestamp :: DATE >= '2023-05-01'
        AND topics [0] :: STRING IN (
            '0xa13c46268c53408442d94eb370f2e476cb7f0fbe027ae5bad73ce13d4469c8b9',
            -- SwapNFTOutPair 721
            '0x7a0e7e58d91fd23a96b0008604db1b2d1cee4aae434e3aad9a20fdd7c0995f89',
            -- SwapNFTInPair 721
            '0xd9c2402e1a067734ae78dab98f06d5b28e8a2d2c6370ec0e6ff8cc2749d050f1',
            -- SwapNFTOutPair 1155
            '0x58e7e2e8d4c949c019e4fe5f6e2a8f10e4e078a8747730386e9a230da8c969f0' --  SwapNFTInPair 1155
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= CURRENT_DATE() - INTERVAL '7 day'
{% endif %}
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
        block_timestamp :: DATE >= '2023-05-01'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= CURRENT_DATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    intra_tx_grouping,
    intra_tx_grouping_new,
    trace_index,
    from_address,
    to_address,
    function_name,
    'sale' AS event_type,
    pool_address AS platform_address,
    'sudoswap' AS platform_name,
    'sudoswap v2' AS platform_exchange_version,
    pair_creation_function,
    pool_address,
    pool_type,
    token_address,
    currency_address,
    nft_address,
    tokenid,
    erc1155_value,
    num_items,
    token_recipient,
    nft_recipient,
    seller_address,
    buyer_address,
    total_price_raw,
    platform_fee_raw,
    creator_fee_raw,
    total_fees_raw,
    output_amount,
    protocol_fee,
    royalty_fee_raw,
    royalty_nft_address,
    royalty_tokenid,
    router_caller,
    swap_tag_fill,
    group_tag_fill,
    delta,
    fee_multiplier,
    protocol_fee_multiplier,
    error,
    new_spot_price,
    trade_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_fee,
    input_data,
    _log_id,
    _inserted_timestamp,
    CONCAT(
        nft_address,
        '-',
        tokenid,
        '-',
        platform_exchange_version,
        '-',
        _log_id
    ) AS nft_log_id
FROM
    final_base
    INNER JOIN raw_logs USING (
        tx_hash,
        intra_tx_grouping_new
    )
    INNER JOIN tx_data USING (tx_hash) qualify(ROW_NUMBER() over(PARTITION BY nft_log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
