{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime']
) }}

WITH sudoswap_tx AS (

    SELECT
        DISTINCT tx_hash
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_timestamp :: DATE >= '2022-04-24'
        AND topics [0] :: STRING IN (
            '0xf06180fdbe95e5193df4dcd1352726b1f04cb58599ce58552cc952447af2ffbb',
            '0xbc479dfc6cb9c1a9d880f987ee4b30fa43dd7f06aec121db685b67d587c93c93',
            '0x3614eb567740a0ee3897c0e2b11ad6a5720d2e4438f9c8accf6c95c24af3a470'
        )

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
raw_tx AS (
    SELECT
        tx_hash,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        origin_function_signature,
        VALUE * pow(
            10,
            18
        ) AS value_adj,
        tx_fee,
        input_data
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        block_timestamp :: DATE >= '2022-04-24'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                sudoswap_tx
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
raw_traces AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        from_address,
        to_address,
        IFF(
            to_address = '0xb16c1342e617a5b6e4b631eb114483fdb289c0a4',
            'platform_fee',
            NULL
        ) AS platform_fee_label,
        eth_value * pow(
            10,
            18
        ) AS eth_value_adj,
        trace_index,
        CASE
            WHEN platform_fee_label = 'platform_fee' THEN LAG(eth_value_adj) over (
                PARTITION BY tx_hash
                ORDER BY
                    trace_index ASC
            )
        END AS sale_amount,
        CASE
            WHEN platform_fee_label = 'platform_fee' THEN LAG(from_address) over (
                PARTITION BY tx_hash
                ORDER BY
                    trace_index ASC
            )
        END AS prev_from_address,
        CASE
            WHEN platform_fee_label = 'platform_fee' THEN LAG(to_address) over (
                PARTITION BY tx_hash
                ORDER BY
                    trace_index ASC
            )
        END AS prev_to_address
    FROM
        {{ ref('silver__traces') }}
    WHERE
        block_timestamp :: DATE >= '2022-04-24'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                sudoswap_tx
        )
        AND identifier != 'CALL_ORIGIN'
        AND trace_status = 'SUCCESS'
        AND tx_status = 'SUCCESS'
        AND eth_value > 0

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
eth_sales_amount AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        from_address,
        to_address,
        trace_index,
        sale_amount,
        IFF(
            to_address = '0xb16c1342e617a5b6e4b631eb114483fdb289c0a4',
            eth_value_adj,
            NULL
        ) AS platform_fee_raw,
        'ETH' AS currency_address,
        prev_from_address AS prev_eth_from_address,
        prev_to_address AS prev_eth_to_address
    FROM
        raw_traces
    WHERE
        platform_fee_raw IS NOT NULL
),
eth_amount_transfers AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        from_address AS eth_from_address,
        to_address AS eth_to_address,
        trace_index,
        IFF(
            sale_amount IS NULL,
            t.value_adj,
            sale_amount
        ) AS sale_amount_raw,
        platform_fee_raw,
        e.currency_address,
        prev_eth_from_address,
        prev_eth_to_address,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) AS eth_sale_index
    FROM
        eth_sales_amount e
        INNER JOIN raw_tx t USING (tx_hash)
),
eth_amount_transfers_count AS (
    SELECT
        tx_hash,
        COUNT(1) AS eth_transfers_total_count
    FROM
        eth_amount_transfers
    GROUP BY
        tx_hash
),
sudo_indicator_logs AS (
    SELECT
        block_number,
        block_timestamp,
        _log_id,
        _inserted_timestamp,
        tx_hash,
        event_index,
        contract_address,
        topics,
        DATA,
        topics [0] :: STRING AS first_topic,
        CASE
            WHEN topics [0] :: STRING IN (
                '0xf06180fdbe95e5193df4dcd1352726b1f04cb58599ce58552cc952447af2ffbb',
                '0x3614eb567740a0ee3897c0e2b11ad6a5720d2e4438f9c8accf6c95c24af3a470',
                '0xbc479dfc6cb9c1a9d880f987ee4b30fa43dd7f06aec121db685b67d587c93c93'
            ) THEN ROW_NUMBER() over (
                PARTITION BY tx_hash
                ORDER BY
                    event_index ASC
            )
            ELSE NULL
        END AS sale_log_indicator
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_timestamp >= '2022-04-24'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                sudoswap_tx
        )

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
total_logs AS (
    SELECT
        *,
        CASE
            WHEN sale_log_indicator IS NULL
            AND topics [3] IS NOT NULL THEN '0x' || SUBSTR(
                topics [1] :: STRING,
                27,
                40
            )
            ELSE NULL
        END AS log_nft_from_address,
        CASE
            WHEN sale_log_indicator IS NULL
            AND topics [3] IS NOT NULL THEN '0x' || SUBSTR(
                topics [2] :: STRING,
                27,
                40
            )
            ELSE NULL
        END AS log_nft_to_address,
        CASE
            WHEN sale_log_indicator IS NULL
            AND topics [3] IS NOT NULL THEN utils.udf_hex_to_int(
                topics [3]
            ) :: STRING
            ELSE NULL
        END AS tokenid,
        CASE
            WHEN sale_log_indicator IS NULL
            AND topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
            AND topics [3] IS NOT NULL THEN LEAD(sale_log_indicator) ignore nulls over (
                PARTITION BY tx_hash
                ORDER BY
                    event_index ASC
            )
            ELSE NULL
        END AS transfers_label,
        CASE
            WHEN sale_log_indicator IS NULL
            AND topics [3] IS NULL
            AND topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
            AND '0x' || SUBSTR(
                topics [2] :: STRING,
                27,
                40
            ) = '0xb16c1342e617a5b6e4b631eb114483fdb289c0a4' THEN LEAD(sale_log_indicator) ignore nulls over (
                PARTITION BY tx_hash
                ORDER BY
                    event_index ASC
            )
            ELSE NULL
        END AS payment_to_platform_label,
        CASE
            WHEN sale_log_indicator IS NULL
            AND topics [3] IS NULL
            AND topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
            AND '0x' || SUBSTR(
                topics [2] :: STRING,
                27,
                40
            ) = '0xb16c1342e617a5b6e4b631eb114483fdb289c0a4' THEN contract_address
            ELSE NULL
        END AS currency_address,
        CASE
            WHEN sale_log_indicator IS NULL
            AND topics [3] IS NULL
            AND topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
            AND '0x' || SUBSTR(
                topics [2] :: STRING,
                27,
                40
            ) = '0xb16c1342e617a5b6e4b631eb114483fdb289c0a4' THEN '0x' || SUBSTR(
                topics [1] :: STRING,
                27,
                40
            )
            ELSE NULL
        END AS payment_from_address,
        CASE
            WHEN sale_log_indicator IS NULL
            AND topics [3] IS NULL
            AND topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
            AND '0x' || SUBSTR(
                topics [2] :: STRING,
                27,
                40
            ) = '0xb16c1342e617a5b6e4b631eb114483fdb289c0a4' THEN utils.udf_hex_to_int(DATA) :: INT
            ELSE NULL
        END AS platform_fee_raw
    FROM
        sudo_indicator_logs
),
sale_log_indicator_raw AS (
    SELECT
        *
    FROM
        total_logs
    WHERE
        sale_log_indicator IS NOT NULL
        AND first_topic != '0xf06180fdbe95e5193df4dcd1352726b1f04cb58599ce58552cc952447af2ffbb'
),
batched_nft_transfers AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l._log_id,
        l._inserted_timestamp,
        l.tx_hash,
        l.event_index,
        l.contract_address AS nft_address,
        r.contract_address AS pool_address,
        l.topics,
        l.log_nft_from_address,
        l.log_nft_to_address,
        l.tokenid,
        l.transfers_label
    FROM
        total_logs l
        INNER JOIN sale_log_indicator_raw r
        ON l.tx_hash = r.tx_hash
        AND l.transfers_label = r.sale_log_indicator
    WHERE
        l.transfers_label IS NOT NULL qualify ROW_NUMBER() over (
            PARTITION BY l.tx_hash,
            nft_address,
            l.tokenid,
            l.transfers_label
            ORDER BY
                l.event_index ASC
        ) = 1
),
token_transfers_raw AS (
    SELECT
        block_number,
        block_timestamp,
        _log_id,
        _inserted_timestamp,
        tx_hash,
        event_index AS token_transfers_index,
        payment_to_platform_label,
        currency_address,
        payment_from_address,
        platform_fee_raw,
        CASE
            WHEN payment_to_platform_label IS NOT NULL THEN LAG(topics) over (
                PARTITION BY tx_hash
                ORDER BY
                    event_index ASC
            )
            ELSE NULL
        END AS prev_topic,
        CASE
            WHEN payment_to_platform_label IS NOT NULL THEN LAG(DATA) over (
                PARTITION BY tx_hash
                ORDER BY
                    event_index ASC
            )
            ELSE NULL
        END AS prev_data,
        '0x' || SUBSTR(
            prev_topic [1] :: STRING,
            27,
            40
        ) AS prev_payment_from_address,
        '0x' || SUBSTR(
            prev_topic [2] :: STRING,
            27,
            40
        ) AS prev_payment_to_address,
        utils.udf_hex_to_int(prev_data) :: INT AS sale_amount_raw
    FROM
        total_logs
    WHERE
        first_topic :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND transfers_label IS NULL
),
token_transfers AS (
    SELECT
        block_number,
        block_timestamp,
        _log_id,
        _inserted_timestamp,
        tx_hash,
        token_transfers_index,
        payment_to_platform_label,
        currency_address,
        payment_from_address,
        '0xb16c1342e617a5b6e4b631eb114483fdb289c0a4' AS payment_to_address,
        platform_fee_raw,
        prev_payment_from_address,
        prev_payment_to_address,
        sale_amount_raw
    FROM
        token_transfers_raw
    WHERE
        payment_to_platform_label IS NOT NULL
),
nft_transfers_x_token_transfers AS (
    -- contains the full table with sales with no token transfers
    SELECT
        n.block_number,
        n.block_timestamp,
        n._log_id,
        n._inserted_timestamp,
        n.tx_hash,
        n.event_index AS nft_transfers_index,
        n.nft_address,
        n.pool_address,
        log_nft_from_address,
        log_nft_to_address,
        tokenid,
        token_transfers_index,
        t.currency_address,
        payment_from_address,
        payment_to_address,
        platform_fee_raw,
        prev_payment_from_address,
        prev_payment_to_address,
        sale_amount_raw,
        transfers_label,
        payment_to_platform_label
    FROM
        batched_nft_transfers n
        LEFT JOIN token_transfers t
        ON n.tx_hash = t.tx_hash
        AND n.transfers_label = t.payment_to_platform_label
),
nft_transfers_x_token_transfers_offer_count AS (
    SELECT
        tx_hash,
        transfers_label,
        COUNT(1) AS offer_count
    FROM
        nft_transfers_x_token_transfers
    WHERE
        token_transfers_index IS NOT NULL
    GROUP BY
        tx_hash,
        transfers_label
),
nft_transfers_x_token_transfers_final AS (
    SELECT
        *,
        platform_fee_raw / offer_count AS platform_fee_raw_adj,
        sale_amount_raw / offer_count AS sale_amount_raw_adj
    FROM
        nft_transfers_x_token_transfers
        INNER JOIN nft_transfers_x_token_transfers_offer_count USING (
            tx_hash,
            transfers_label
        )
    WHERE
        token_transfers_index IS NOT NULL
),
nft_transfers_for_eth_transfers_count AS (
    SELECT
        tx_hash,
        COUNT(1) AS nft_transfers_total_count
    FROM
        nft_transfers_x_token_transfers
    WHERE
        token_transfers_index IS NULL
    GROUP BY
        tx_hash
),
nft_transfers_eth_batched_indicator AS (
    SELECT
        n.tx_hash,
        nft_transfers_total_count,
        eth_transfers_total_count,
        IFF(
            nft_transfers_total_count = eth_transfers_total_count,
            'single',
            'batch'
        ) AS batch_indicator
    FROM
        nft_transfers_for_eth_transfers_count n
        INNER JOIN eth_amount_transfers_count e USING (tx_hash)
),
nft_transfers_for_eth_transfers_raw AS (
    SELECT
        *
    FROM
        nft_transfers_x_token_transfers n
        INNER JOIN nft_transfers_eth_batched_indicator b USING (tx_hash)
    WHERE
        token_transfers_index IS NULL
),
nft_transfers_for_eth_transfers_single AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                nft_transfers_index ASC
        ) AS nft_transfers_for_eth_index
    FROM
        nft_transfers_for_eth_transfers_raw
    WHERE
        batch_indicator = 'single'
),
nft_transfers_x_eth_transfers_single AS (
    SELECT
        n.block_number,
        n.block_timestamp,
        n._log_id,
        n._inserted_timestamp,
        n.tx_hash,
        nft_transfers_index,
        n.nft_address,
        n.pool_address,
        log_nft_from_address,
        log_nft_to_address,
        tokenid,
        eth_from_address,
        eth_to_address,
        trace_index,
        e.sale_amount_raw,
        e.platform_fee_raw,
        e.currency_address,
        prev_eth_from_address,
        prev_eth_to_address,
        nft_transfers_for_eth_index,
        eth_sale_index
    FROM
        nft_transfers_for_eth_transfers_single n
        INNER JOIN eth_amount_transfers e
        ON n.tx_hash = e.tx_hash
        AND n.nft_transfers_for_eth_index = e.eth_sale_index
    WHERE
        trace_index IS NOT NULL
        AND e.currency_address IS NOT NULL
),
nft_transfers_for_eth_transfers_batch_raw AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            transfers_label
            ORDER BY
                nft_transfers_index ASC
        ) AS row_number_within_densed,
        DENSE_RANK() over (
            PARTITION BY tx_hash
            ORDER BY
                transfers_label ASC
        ) AS densed_rank
    FROM
        nft_transfers_for_eth_transfers_raw
    WHERE
        batch_indicator = 'batch'
),
nft_transfers_for_eth_transfers_batch_offer_count AS (
    SELECT
        tx_hash,
        transfers_label,
        MAX(row_number_within_densed) AS offer_count
    FROM
        nft_transfers_for_eth_transfers_batch_raw
    GROUP BY
        tx_hash,
        transfers_label
),
nft_transfers_x_eth_transfers_batch AS (
    SELECT
        n.block_number,
        n.block_timestamp,
        n._log_id,
        n._inserted_timestamp,
        n.tx_hash,
        nft_transfers_index,
        n.nft_address,
        n.pool_address,
        log_nft_from_address,
        log_nft_to_address,
        tokenid,
        eth_from_address,
        eth_to_address,
        trace_index,
        e.sale_amount_raw,
        e.platform_fee_raw,
        o.offer_count,
        e.sale_amount_raw / o.offer_count AS sale_amount_raw_adj,
        e.platform_fee_raw / o.offer_count AS platform_fee_raw_adj,
        e.currency_address,
        prev_eth_from_address,
        prev_eth_to_address,
        row_number_within_densed,
        densed_rank,
        eth_sale_index
    FROM
        nft_transfers_for_eth_transfers_batch_raw n
        INNER JOIN eth_amount_transfers e
        ON n.tx_hash = e.tx_hash
        AND n.densed_rank = e.eth_sale_index
        INNER JOIN nft_transfers_for_eth_transfers_batch_offer_count o
        ON n.tx_hash = o.tx_hash
        AND n.transfers_label = o.transfers_label
),
all_sales_base AS (
    -- batched & single tokens, eth single and eth batched
    SELECT
        block_number,
        block_timestamp,
        _log_id,
        _inserted_timestamp,
        tx_hash,
        nft_transfers_index,
        nft_address,
        pool_address,
        log_nft_from_address,
        log_nft_to_address,
        tokenid,
        token_transfers_index AS payment_index,
        currency_address,
        payment_from_address,
        payment_to_address,
        platform_fee_raw_adj AS platform_fee_raw,
        prev_payment_from_address,
        prev_payment_to_address,
        sale_amount_raw_adj AS sale_amount_raw,
        offer_count,
        IFF(
            offer_count > 1,
            TRUE,
            FALSE
        ) AS is_price_estimated
    FROM
        nft_transfers_x_token_transfers_final
    WHERE
        currency_address IS NOT NULL
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        _log_id,
        _inserted_timestamp,
        tx_hash,
        nft_transfers_index,
        nft_address,
        pool_address,
        log_nft_from_address,
        log_nft_to_address,
        tokenid,
        trace_index AS payment_index,
        currency_address,
        eth_from_address AS payment_from_address,
        eth_to_address AS payment_to_address,
        platform_fee_raw,
        prev_eth_from_address AS prev_payment_from_address,
        prev_eth_to_address AS prev_payment_to_address,
        sale_amount_raw,
        1 AS offer_count,
        FALSE AS is_price_estimated
    FROM
        nft_transfers_x_eth_transfers_single
    WHERE
        currency_address IS NOT NULL
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        _log_id,
        _inserted_timestamp,
        tx_hash,
        nft_transfers_index,
        nft_address,
        pool_address,
        log_nft_from_address,
        log_nft_to_address,
        tokenid,
        trace_index AS payment_index,
        currency_address,
        eth_from_address AS payment_from_address,
        eth_to_address AS payment_to_address,
        platform_fee_raw_adj AS platform_fee_raw,
        prev_eth_from_address AS prev_payment_from_address,
        prev_eth_to_address AS prev_payment_to_address,
        sale_amount_raw_adj AS sale_amount_raw,
        offer_count,
        IFF(
            offer_count > 1,
            TRUE,
            FALSE
        ) AS is_price_estimated
    FROM
        nft_transfers_x_eth_transfers_batch
    WHERE
        currency_address IS NOT NULL
)
SELECT
    b.tx_hash,
    b.block_number,
    b.block_timestamp,
    nft_transfers_index AS event_index,
    'SpotPriceUpdate' AS event_name,
    'sale' AS event_type,
    '0x2b2e8cda09bba9660dca5cb6233787738ad68329' AS platform_address,
    'sudoswap' AS platform_name,
    'pair router' AS platform_exchange_version,
    nft_transfers_index,
    log_nft_from_address,
    log_nft_to_address,
    log_nft_from_address AS seller_address,
    log_nft_to_address AS buyer_address,
    currency_address,
    sale_amount_raw,
    sale_amount_raw + platform_fee_raw AS total_price_raw,
    platform_fee_raw AS total_fees_raw,
    platform_fee_raw,
    0 AS creator_fee_raw,
    nft_address,
    pool_address,
    tokenid AS tokenId,
    NULL AS erc1155_value,
    payment_index,
    payment_from_address,
    payment_to_address,
    prev_payment_from_address,
    prev_payment_to_address,
    offer_count,
    is_price_estimated,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_fee,
    input_data,
    b._log_id,
    CONCAT(
        nft_address,
        '-',
        tokenid,
        '-',
        platform_exchange_version,
        '-',
        b._log_id
    ) AS nft_log_id,
    b._inserted_timestamp
FROM
    all_sales_base b
    INNER JOIN raw_tx t
    ON b.tx_hash = t.tx_hash qualify(ROW_NUMBER() over(PARTITION BY nft_log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
