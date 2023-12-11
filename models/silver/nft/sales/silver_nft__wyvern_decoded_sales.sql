{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['stale']
) }}

WITH raw_logs AS (

    SELECT
        block_number,
        block_timestamp,
        _log_id,
        _inserted_timestamp,
        tx_hash,
        event_index,
        event_name,
        contract_address,
        decoded_flat :price :: INT AS total_price_undivided,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS intra_tx_index,
        LAG(event_index) over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS prev_event_index
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_timestamp :: DATE BETWEEN '2018-06-12'
        AND '2022-08-02'
        AND event_name = 'OrdersMatched'
        AND contract_address IN (
            '0x7f268357a8c2552623316e2562d90e642bb538e5',
            '0x7be8076f4ea4a4ad08075c2508e481d6c946d12b'
        )
),
raw_traces AS (
    SELECT
        tx_hash,
        trace_index,
        TYPE,
        from_address,
        to_address,
        eth_value,
        input,
        LEFT(
            input,
            10
        ) AS function_sig,
        regexp_substr_all(SUBSTR(input, 11, len(input)), '.{64}') AS segmented_data
    FROM
        {{ ref('silver__traces') }}
    WHERE
        block_timestamp :: DATE BETWEEN '2018-06-12'
        AND '2022-08-02'
        AND trace_status = 'SUCCESS'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                raw_logs
        )
),
sale_details AS (
    SELECT
        tx_hash,
        trace_index,
        from_address,
        to_address,
        input,
        eth_value,
        segmented_data,
        '0x' || SUBSTR(
            segmented_data [0] :: STRING,
            25
        ) AS platform_address,
        '0x' || SUBSTR(
            segmented_data [1] :: STRING,
            25
        ) AS trace_buyer_address,
        '0x' || SUBSTR(
            segmented_data [2] :: STRING,
            25
        ) AS seller_address_temp,
        IFF(
            seller_address_temp = '0x0000000000000000000000000000000000000000',
            '0x' || SUBSTR(
                segmented_data [8] :: STRING,
                25
            ),
            seller_address_temp
        ) AS trace_seller_address,
        '0x' || SUBSTR(
            segmented_data [3] :: STRING,
            25
        ) AS fee_receiver_three,
        '0x' || SUBSTR(
            segmented_data [10] :: STRING,
            25
        ) AS fee_receiver_ten,
        IFF(
            fee_receiver_ten = '0x0000000000000000000000000000000000000000',
            'bid_won',
            'sale'
        ) AS event_type,
        CASE
            WHEN fee_receiver_ten != '0x0000000000000000000000000000000000000000' THEN IFF(
                (
                    utils.udf_hex_to_int(
                        segmented_data [23] :: STRING
                    ) + utils.udf_hex_to_int(
                        segmented_data [24] :: STRING
                    )
                ) < 250,
                (
                    utils.udf_hex_to_int(
                        segmented_data [23] :: STRING
                    ) + utils.udf_hex_to_int(
                        segmented_data [24] :: STRING
                    )
                ),
                (
                    utils.udf_hex_to_int(
                        segmented_data [23] :: STRING
                    ) + utils.udf_hex_to_int(
                        segmented_data [24] :: STRING
                    ) - 250
                )
            )
            WHEN fee_receiver_three != '0x0000000000000000000000000000000000000000' THEN IFF(
                (
                    utils.udf_hex_to_int(
                        segmented_data [14] :: STRING
                    ) + utils.udf_hex_to_int(
                        segmented_data [15] :: STRING
                    )
                ) < 250,
                (
                    utils.udf_hex_to_int(
                        segmented_data [14] :: STRING
                    ) + utils.udf_hex_to_int(
                        segmented_data [15] :: STRING
                    )
                ),
                (
                    utils.udf_hex_to_int(
                        segmented_data [14] :: STRING
                    ) + utils.udf_hex_to_int(
                        segmented_data [15] :: STRING
                    ) - 250
                )
            )
            ELSE 0
        END AS creator_fee_bps,
        CASE
            WHEN fee_receiver_ten != '0x0000000000000000000000000000000000000000' -- bid won
            THEN IFF(
                (
                    utils.udf_hex_to_int(
                        segmented_data [23] :: STRING
                    ) + utils.udf_hex_to_int(
                        segmented_data [24] :: STRING
                    )
                ) < 250,
                0,
                250
            )
            WHEN fee_receiver_three != '0x0000000000000000000000000000000000000000' -- sale
            THEN IFF(
                (
                    utils.udf_hex_to_int(
                        segmented_data [14] :: STRING
                    ) + utils.udf_hex_to_int(
                        segmented_data [15] :: STRING
                    )
                ) < 250,
                0,
                250
            )
            ELSE 0
        END AS platform_fee_bps,
        '0x' || SUBSTR(
            segmented_data [4] :: STRING,
            25
        ) AS nft_address,
        '0x' || SUBSTR(
            segmented_data [6] :: STRING,
            25
        ) AS currency_address,
        IFF(
            nft_address = '0xc99f70bfd82fb7c8f8191fdfbfb735606b15e5c5',
            'bundle',
            'single'
        ) AS sale_type,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) AS intra_tx_index
    FROM
        raw_traces
    WHERE
        to_address IN (
            '0x7be8076f4ea4a4ad08075c2508e481d6c946d12b',
            '0x7f268357a8c2552623316e2562d90e642bb538e5'
        )
        AND function_sig = '0xab834bab'
),
base_sale_details AS (
    SELECT
        tx_hash,
        intra_tx_index,
        segmented_data,
        event_index,
        prev_event_index,
        event_name,
        event_type,
        trace_index,
        LEAD(trace_index) over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) AS next_trace_index,
        platform_address,
        sale_type,
        trace_buyer_address,
        trace_seller_address,
        nft_address,
        currency_address,
        total_price_undivided,
        creator_fee_bps,
        platform_fee_bps,
        total_price_undivided * creator_fee_bps / 1e4 AS creator_fee_undiv,
        total_price_undivided * platform_fee_bps / 1e4 AS platform_fee_undiv,
        block_number,
        block_timestamp,
        _log_id,
        _inserted_timestamp
    FROM
        sale_details
        INNER JOIN raw_logs USING (
            tx_hash,
            intra_tx_index
        )
),
merkle_validator_raw AS (
    SELECT
        tx_hash,
        trace_index,
        function_sig,
        '0x' || SUBSTR(
            segmented_data [0] :: STRING,
            25
        ) AS nft_from_address,
        '0x' || SUBSTR(
            segmented_data [1] :: STRING,
            25
        ) AS nft_to_address,
        '0x' || SUBSTR(
            segmented_data [2] :: STRING,
            25
        ) AS nft_address,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) AS tokenid,
        IFF(
            function_sig = '0x96809f90',
            utils.udf_hex_to_int(
                segmented_data [4] :: STRING
            ),
            NULL
        ) AS erc1155_value,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) AS intra_tx_index
    FROM
        raw_traces
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                sale_details
            WHERE
                nft_address = '0xbaf2127b49fc93cbca6269fade0f7f31df4c88a7'
        )
        AND to_address = '0xbaf2127b49fc93cbca6269fade0f7f31df4c88a7'
        AND function_sig IN (
            '0xfb16a595',
            '0x96809f90'
        )
),
merkle_sale_details AS (
    SELECT
        s.tx_hash,
        s.intra_tx_index,
        s.trace_index,
        s.event_index,
        event_type,
        sale_type,
        platform_address,
        trace_buyer_address,
        trace_seller_address,
        nft_from_address,
        nft_to_address,
        m.nft_address,
        tokenid,
        erc1155_value,
        currency_address,
        total_price_undivided,
        creator_fee_bps,
        platform_fee_bps,
        creator_fee_undiv,
        platform_fee_undiv,
        block_number,
        block_timestamp,
        _log_id,
        _inserted_timestamp
    FROM
        base_sale_details s
        INNER JOIN merkle_validator_raw m
        ON s.tx_hash = m.tx_hash
        AND m.trace_index > s.trace_index
        AND (
            m.trace_index < next_trace_index
            OR next_trace_index IS NULL
        )
    WHERE
        s.nft_address = '0xbaf2127b49fc93cbca6269fade0f7f31df4c88a7'
),
nft_transfers_raw AS (
    SELECT
        tx_hash,
        event_index,
        from_address,
        to_address,
        contract_address,
        tokenid,
        erc1155_value
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        block_timestamp :: DATE BETWEEN '2018-06-12'
        AND '2022-08-02'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                sale_details
        )
),
single_transfer AS (
    SELECT
        s.tx_hash,
        intra_tx_index,
        trace_index,
        s.event_index,
        event_type,
        sale_type,
        platform_address,
        trace_buyer_address,
        trace_seller_address,
        n.from_address AS nft_from_address,
        n.to_address AS nft_to_address,
        nft_address,
        tokenid,
        erc1155_value,
        currency_address,
        total_price_undivided,
        creator_fee_bps,
        platform_fee_bps,
        creator_fee_undiv,
        platform_fee_undiv,
        block_number,
        block_timestamp,
        _log_id,
        _inserted_timestamp
    FROM
        base_sale_details s
        INNER JOIN nft_transfers_raw n
        ON s.tx_hash = n.tx_hash
        AND s.nft_address = n.contract_address -- doing this because the maker and taker can be different than nft receivers
        AND (
            (
                event_type = 'sale'
                AND n.from_address = s.trace_seller_address
            )
            OR (
                event_type = 'bid_won'
                AND n.to_address = s.trace_buyer_address
            )
        )
        AND n.event_index < s.event_index
        AND (
            prev_event_index IS NULL
            OR n.event_index > s.prev_event_index
        )
    WHERE
        s.nft_address NOT IN (
            '0xc99f70bfd82fb7c8f8191fdfbfb735606b15e5c5',
            '0xbaf2127b49fc93cbca6269fade0f7f31df4c88a7'
        )
),
raw_atomicize AS (
    SELECT
        tx_hash,
        from_address,
        to_address,
        TYPE,
        input,
        function_sig,
        trace_index,
        regexp_substr_all(SUBSTR(input, 11, len(input)), '.{64}') AS segmented_data,
        CASE
            WHEN to_address = '0xc99f70bfd82fb7c8f8191fdfbfb735606b15e5c5' THEN 1
            ELSE NULL
        END AS multi_mark,
        LAG(multi_mark) ignore nulls over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) AS multi_mark_fill
    FROM
        raw_traces
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                sale_details
            WHERE
                nft_address = '0xc99f70bfd82fb7c8f8191fdfbfb735606b15e5c5'
        )
),
atomicize_nft_address AS (
    SELECT
        tx_hash,
        trace_index AS atomicize_trace_index,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) / 32 AS index_of_nft_address_list,
        utils.udf_hex_to_int(
            segmented_data [index_of_nft_address_list] :: STRING
        ) AS nft_count,
        '0x' || SUBSTR(
            VALUE :: STRING,
            25
        ) AS nft_address,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) AS nft_transfers_index
    FROM
        raw_atomicize,
        LATERAL FLATTEN (
            input => segmented_data
        )
    WHERE
        INDEX BETWEEN (
            index_of_nft_address_list + 1
        )
        AND (
            index_of_nft_address_list + nft_count
        )
        AND to_address = '0xc99f70bfd82fb7c8f8191fdfbfb735606b15e5c5'
        AND function_sig = '0x68f0bcaa'
),
transfers_details AS (
    SELECT
        tx_hash,
        trace_index,
        multi_mark_fill,
        function_sig,
        '0x' || SUBSTR(
            segmented_data [0] :: STRING,
            25
        ) AS nft_from_address,
        '0x' || SUBSTR(
            segmented_data [1] :: STRING,
            25
        ) AS nft_to_address,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: STRING AS tokenid,
        COALESCE(
            utils.udf_hex_to_int(
                segmented_data [3] :: STRING
            ) :: STRING,
            NULL
        ) AS quantity,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) AS nft_transfers_index
    FROM
        raw_atomicize
    WHERE
        multi_mark_fill IS NOT NULL
        AND function_sig IN (
            '0x23b872dd',
            '0xf242432a'
        )
        AND TYPE = 'CALL'
),
batch_transfer_traces AS (
    SELECT
        tx_hash,
        nft_transfers_index,
        atomicize_trace_index,
        nft_count,
        trace_index,
        nft_address,
        nft_from_address,
        nft_to_address,
        tokenid,
        quantity
    FROM
        atomicize_nft_address
        INNER JOIN transfers_details USING (
            tx_hash,
            nft_transfers_index
        )
),
batch_transfer AS (
    SELECT
        b.tx_hash,
        intra_tx_index,
        s.trace_index,
        s.event_index,
        event_type,
        sale_type,
        platform_address,
        trace_buyer_address,
        trace_seller_address,
        nft_from_address,
        nft_to_address,
        b.nft_address,
        b.tokenid,
        b.quantity AS erc1155_value,
        currency_address,
        total_price_undivided,
        creator_fee_bps,
        platform_fee_bps,
        creator_fee_undiv,
        platform_fee_undiv,
        block_number,
        block_timestamp,
        _log_id,
        _inserted_timestamp
    FROM
        batch_transfer_traces b
        INNER JOIN base_sale_details s
        ON s.tx_hash = b.tx_hash
        AND b.atomicize_trace_index > s.trace_index
    WHERE
        s.nft_address = '0xc99f70bfd82fb7c8f8191fdfbfb735606b15e5c5'
),
custom_atomicizer AS (
    SELECT
        n.tx_hash,
        intra_tx_index,
        trace_index,
        s.event_index,
        event_type,
        sale_type,
        platform_address,
        trace_buyer_address,
        trace_seller_address,
        n.from_address AS nft_from_address,
        n.to_address AS nft_to_address,
        n.contract_address AS nft_address,
        n.tokenid,
        n.erc1155_value,
        currency_address,
        total_price_undivided,
        creator_fee_bps,
        platform_fee_bps,
        creator_fee_undiv,
        platform_fee_undiv,
        block_number,
        block_timestamp,
        _log_id,
        _inserted_timestamp
    FROM
        base_sale_details s
        INNER JOIN nft_transfers_raw n
        ON s.tx_hash = n.tx_hash
        AND (
            (
                event_type = 'sale'
                AND n.from_address = s.trace_seller_address
            )
            OR (
                event_type = 'bid_won'
                AND n.to_address = s.trace_buyer_address
            )
            OR (
                event_type = 'sale'
                AND n.from_address = '0x0000000000000000000000000000000000000000'
                AND n.to_address = s.trace_buyer_address
            )
        )
    WHERE
        s.tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                batch_transfer_traces
        )
        AND s.tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                merkle_sale_details
        )
        AND s.tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                single_transfer
        )
),
all_nft_transfers AS (
    SELECT
        tx_hash,
        intra_tx_index,
        trace_index,
        event_index,
        event_type,
        sale_type,
        platform_address,
        trace_buyer_address,
        trace_seller_address,
        nft_from_address,
        nft_to_address,
        nft_address,
        tokenid,
        erc1155_value,
        currency_address,
        total_price_undivided,
        creator_fee_bps,
        platform_fee_bps,
        creator_fee_undiv,
        platform_fee_undiv,
        'single' AS TYPE,
        block_number,
        block_timestamp,
        _log_id,
        _inserted_timestamp
    FROM
        single_transfer
    UNION ALL
    SELECT
        tx_hash,
        intra_tx_index,
        trace_index,
        event_index,
        event_type,
        sale_type,
        platform_address,
        trace_buyer_address,
        trace_seller_address,
        nft_from_address,
        nft_to_address,
        nft_address,
        tokenid,
        erc1155_value,
        currency_address,
        total_price_undivided,
        creator_fee_bps,
        platform_fee_bps,
        creator_fee_undiv,
        platform_fee_undiv,
        'batch' AS TYPE,
        block_number,
        block_timestamp,
        _log_id,
        _inserted_timestamp
    FROM
        batch_transfer
    UNION ALL
    SELECT
        tx_hash,
        intra_tx_index,
        trace_index,
        event_index,
        event_type,
        sale_type,
        platform_address,
        trace_buyer_address,
        trace_seller_address,
        nft_from_address,
        nft_to_address,
        nft_address,
        tokenid,
        erc1155_value,
        currency_address,
        total_price_undivided,
        creator_fee_bps,
        platform_fee_bps,
        creator_fee_undiv,
        platform_fee_undiv,
        'merkle' AS TYPE,
        block_number,
        block_timestamp,
        _log_id,
        _inserted_timestamp
    FROM
        merkle_sale_details
    UNION ALL
    SELECT
        tx_hash,
        intra_tx_index,
        trace_index,
        event_index,
        event_type,
        sale_type,
        platform_address,
        trace_buyer_address,
        trace_seller_address,
        nft_from_address,
        nft_to_address,
        nft_address,
        tokenid,
        erc1155_value,
        currency_address,
        total_price_undivided,
        creator_fee_bps,
        platform_fee_bps,
        creator_fee_undiv,
        platform_fee_undiv,
        'custom_atomicizer' AS TYPE,
        block_number,
        block_timestamp,
        _log_id,
        _inserted_timestamp
    FROM
        custom_atomicizer
),
base_sales AS (
    SELECT
        tx_hash,
        intra_tx_index,
        trace_index,
        event_index,
        event_type,
        sale_type,
        platform_address,
        trace_buyer_address,
        trace_seller_address,
        nft_from_address,
        nft_to_address,
        nft_address,
        tokenid,
        erc1155_value,
        currency_address,
        total_price_undivided,
        creator_fee_bps,
        platform_fee_bps,
        creator_fee_undiv,
        platform_fee_undiv,
        1 AS quantity,
        total_price_undivided / SUM(quantity) over (
            PARTITION BY tx_hash,
            intra_tx_index
            ORDER BY
                intra_tx_index ASC
        ) AS total_price_raw,
        creator_fee_undiv / SUM(quantity) over (
            PARTITION BY tx_hash,
            intra_tx_index
            ORDER BY
                intra_tx_index ASC
        ) AS creator_fee_raw,
        platform_fee_undiv / SUM(quantity) over (
            PARTITION BY tx_hash,
            intra_tx_index
            ORDER BY
                intra_tx_index ASC
        ) AS platform_fee_raw,
        TYPE,
        block_number,
        block_timestamp,
        _log_id,
        _inserted_timestamp
    FROM
        all_nft_transfers
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
        block_timestamp :: DATE BETWEEN '2018-06-12'
        AND '2022-08-02'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                base_sales
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
    event_index,
    intra_tx_index,
    trace_index,
    event_type,
    sale_type,
    platform_address,
    'opensea' AS platform_name,
    IFF(
        platform_address = '0x7be8076f4ea4a4ad08075c2508e481d6c946d12b',
        'wyvern_v1',
        'wyvern_v2'
    ) AS platform_exchange_version,
    trace_buyer_address,
    trace_seller_address,
    nft_from_address,
    nft_to_address,
    nft_from_address AS seller_address,
    nft_to_address AS buyer_address,
    nft_address,
    tokenid,
    erc1155_value,
    IFF(
        currency_address = '0x0000000000000000000000000000000000000000',
        'ETH',
        currency_address
    ) AS currency_address,
    total_price_undivided,
    creator_fee_bps,
    platform_fee_bps,
    creator_fee_undiv,
    platform_fee_undiv,
    quantity,
    total_price_raw,
    creator_fee_raw,
    platform_fee_raw,
    creator_fee_raw + platform_fee_raw AS total_fees_raw,
    TYPE,
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
    base_sales
    INNER JOIN tx_data USING (tx_hash) qualify (ROW_NUMBER() over(PARTITION BY nft_log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
