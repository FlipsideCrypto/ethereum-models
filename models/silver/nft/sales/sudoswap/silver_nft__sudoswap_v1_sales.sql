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
        pool_type
    FROM
        {{ ref('silver_nft__sudoswap_v1_pools') }}
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
        pool_type,
        input,
        output,
        IFF(
            function_name IN (
                'swapTokenForSpecificNFTs',
                'swapNFTsForToken',
                'swapTokenForAnyNFTs',
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
                'swapTokenForAnyNFTs',
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
        block_timestamp :: DATE >= '2022-04-24'
        AND to_address IN (
            '0x08ce97807a81896e85841d74fb7e7b065ab3ef05',
            -- swap contracts
            '0xcd80c916b1194beb48abf007d0b79a7238436d56',
            '0xd42638863462d2f21bb7d4275d7637ee5d5541eb',
            '0x92de3a1511ef22abcf3526c302159882a4755b22',
            '0x7942e264e21c5e6cbba45fe50785a15d3beb1da0',
            -- bonding curve
            '0x5b6ac51d9b1cede0068a1b26533cace807f883ee',
            '0x432f962d8209781da23fb37b6b59ee15de7d9841'
        )
        AND LEFT(
            input,
            10
        ) IN (
            '0x6d8b99f7',
            -- swapTokenForSpecificNFTs
            '0xb1d3f1c1',
            -- swapNFTsForToken
            '0x28b8aee1',
            --swapTokenForAnyNFTs
            '0x7ca542ac',
            -- getBuyInfo
            '0x097cc63d',
            -- getSellInfo
            '0xa5cb2b91',
            -- getBuyNFTQuote
            '0x0c295e56' -- getSellNFTQuote
        )
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
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
            'swapNFTsForToken',
            'swapTokenForAnyNFTs'
        )
),
all_swaps AS (
    SELECT
        *,
        decoded_data :decoded_input_data :isRouter AS is_router,
        decoded_data :decoded_input_data :nftIds AS nft_ids,
        decoded_data :decoded_input_data :routerCaller :: STRING AS router_caller,
        CASE
            WHEN function_name = 'swapNFTsForToken' THEN decoded_data :decoded_input_data :tokenRecipient :: STRING
            ELSE NULL
        END AS token_recipient,
        CASE
            WHEN function_name IN (
                'swapTokenForSpecificNFTs',
                'swapTokenForAnyNFTs'
            ) THEN decoded_data :decoded_input_data :nftRecipient :: STRING
            ELSE NULL
        END AS nft_recipient,
        CASE
            WHEN function_name = 'swapNFTsForToken' THEN decoded_data :decoded_output_data :outputAmount :: INT
            ELSE decoded_data :decoded_output_data :inputAmount :: INT
        END AS output_amount
    FROM
        base_swap_filtered
    WHERE
        function_name IN (
            'swapNFTsForToken',
            'swapTokenForSpecificNFTs',
            'swapTokenForAnyNFTs'
        )
),
get_buysell_info AS (
    SELECT
        tx_hash,
        intra_tx_grouping,
        decoded_data :decoded_input_data :delta :: INT AS delta,
        decoded_data :decoded_input_data :feeMultiplier :: INT AS fee_multiplier,
        decoded_data :decoded_input_data :numItems :: INT AS num_items,
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
        nft_ids,
        num_items,
        token_recipient,
        nft_recipient,
        output_amount,
        protocol_fee,
        0 AS creator_fee_raw,
        router_caller,
        swap_tag_fill,
        group_tag_fill,
        delta,
        fee_multiplier,
        protocol_fee_multiplier,
        error,
        new_spot_price,
        trade_fee
    FROM
        all_swaps
        INNER JOIN get_buysell_info USING (
            tx_hash,
            intra_tx_grouping
        )
    WHERE
        trace_status = 'SUCCESS'
),
nft_transfers AS (
    SELECT
        tx_hash,
        from_address AS seller_address,
        to_address AS buyer_address,
        contract_address AS nft_address,
        tokenid,
        erc1155_value
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        block_timestamp :: DATE >= '2022-04-24'
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
        nft_ids,
        nft_address,
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
            WHEN function_name = 'swapNFTsForToken' THEN (output_amount + COALESCE(protocol_fee, 0) + creator_fee_raw) / num_items
            ELSE output_amount / num_items
        END AS total_price_raw,
        COALESCE(
            protocol_fee,
            0
        ) / num_items AS platform_fee_raw,
        creator_fee_raw,
        platform_fee_raw + creator_fee_raw AS total_fees_raw,
        output_amount,
        protocol_fee,
        router_caller,
        swap_tag_fill,
        group_tag_fill,
        delta,
        fee_multiplier,
        protocol_fee_multiplier,
        error,
        new_spot_price,
        trade_fee / num_items AS trade_fee
    FROM
        combined_base
),
base_721_specific_swap AS (
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
        currency_address,
        nft_address,
        VALUE :: STRING AS tokenid,
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
        router_caller,
        swap_tag_fill,
        group_tag_fill,
        delta,
        fee_multiplier,
        protocol_fee_multiplier,
        error,
        new_spot_price,
        trade_fee
    FROM
        base_721,
        LATERAL FLATTEN (
            input => nft_ids
        )
    WHERE
        function_name IN (
            'swapNFTsForToken',
            'swapTokenForSpecificNFTs'
        )
),
base_721_nonspecific_swap AS (
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
        router_caller,
        swap_tag_fill,
        group_tag_fill,
        delta,
        fee_multiplier,
        protocol_fee_multiplier,
        error,
        new_spot_price,
        trade_fee
    FROM
        base_721
        LEFT JOIN nft_transfers USING (
            tx_hash,
            seller_address,
            buyer_address
        )
    WHERE
        function_name IN (
            'swapTokenForAnyNFTs'
        )
),
final_base AS (
    SELECT
        *
    FROM
        base_721_specific_swap
    UNION ALL
    SELECT
        *
    FROM
        base_721_nonspecific_swap
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
        ) AS intra_tx_grouping
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address IN (
            SELECT
                pool_address
            FROM
                pools
        )
        AND block_timestamp :: DATE >= '2022-04-24'
        AND topics [0] :: STRING IN (
            '0xbc479dfc6cb9c1a9d880f987ee4b30fa43dd7f06aec121db685b67d587c93c93',
            -- SwapNFTOutPair
            '0x3614eb567740a0ee3897c0e2b11ad6a5720d2e4438f9c8accf6c95c24af3a470' -- SwapNFTInPair
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
        block_timestamp :: DATE >= '2022-04-24'

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
    intra_tx_grouping,
    trace_index,
    from_address,
    to_address,
    function_name,
    'sale' AS event_type,
    pool_address AS platform_address,
    'sudoswap' AS platform_name,
    'sudoswap v1' AS platform_exchange_version,
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
        intra_tx_grouping
    )
    INNER JOIN tx_data USING (tx_hash) qualify(ROW_NUMBER() over(PARTITION BY nft_log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
