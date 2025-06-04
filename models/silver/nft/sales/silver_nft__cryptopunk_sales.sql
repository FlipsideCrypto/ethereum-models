{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','nft','curated']
) }}

WITH raw_traces AS (

    SELECT
        block_number,
        block_timestamp,
        modified_timestamp AS _inserted_timestamp,
        tx_hash,
        trace_index,
        from_address,
        to_address,
        SUBSTR(
            input,
            1,
            10
        ) AS function_sig,
        IFF(
            function_sig = '0x8264fe98',
            'buyPunk',
            'acceptBidForPunk'
        ) AS function_name,
        regexp_substr_all(SUBSTR(input, 11, len(input)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: STRING AS tokenid,
        IFF(
            function_name = 'acceptBidForPunk',
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) :: INT,
            0
        ) AS accepted_bid_price,
        TYPE,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) AS buy_index
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        block_timestamp :: DATE >= '2017-06-20'
        AND to_address = '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb' -- cryptopunksMarket
        AND tx_succeeded
        AND trace_succeeded
        AND function_sig IN (
            '0x23165b75',
            '0x8264fe98'
        ) -- buyPunk, acceptBidForPunk
        AND TYPE = 'CALL'

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
raw_logs AS (
    SELECT
        block_number,
        tx_hash,
        event_index,
        topics,
        DATA,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2017-06-20'
        AND contract_address = '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb'
        AND topics [0] :: STRING IN (
            '0x58e5d5a525e3b40bc15abaa38b5882678db1ee68befd2f60bafe3a7fd06db9e3' -- punk bought
        )
        AND tx_succeeded

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
punk_bought_logs AS (
    SELECT
        tx_hash,
        event_index,
        utils.udf_hex_to_int(
            topics [1] :: STRING
        ) :: STRING AS tokenid,
        '0x' || SUBSTR(
            topics [2] :: STRING,
            27
        ) AS punk_from_address,
        '0x' || SUBSTR(
            topics [3] :: STRING,
            27
        ) AS punk_to_address,
        IFF(
            DATA = '0x0000000000000000000000000000000000000000000000000000000000000000',
            0,
            utils.udf_hex_to_int(
                DATA :: STRING
            ) :: INT
        ) AS VALUE,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS buy_index,
        _log_id
    FROM
        raw_logs
),
base AS (
    SELECT
        block_number,
        block_timestamp,
        t.tx_hash,
        buy_index,
        trace_index,
        l.event_index,
        to_address AS platform_address,
        function_sig,
        function_name,
        IFF(
            function_name = 'buyPunk',
            'sale',
            'bid_won'
        ) AS event_type,
        segmented_data,
        '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb' AS nft_address,
        t.tokenid,
        punk_from_address,
        punk_to_address,
        VALUE,
        accepted_bid_price,
        _log_id,
        _inserted_timestamp
    FROM
        raw_traces t
        INNER JOIN punk_bought_logs l USING (
            tx_hash,
            buy_index
        )
),
latest_punk_bids AS (
    SELECT
        t.tx_hash,
        t.buy_index,
        t.block_number,
        b.block_number AS bid_block_number,
        b.tx_hash AS bid_tx_hash,
        t.tokenid,
        bid_value,
        accepted_bid_price,
        bid_from_address
    FROM
        {{ ref('silver_nft__cryptopunk_bids') }}
        b
        INNER JOIN base t
        ON t.tokenid = b.bid_tokenid
        AND t.block_number >= b.block_number
    WHERE
        t.function_name = 'acceptBidForPunk' qualify ROW_NUMBER() over (
            PARTITION BY t.tx_hash,
            t.tokenid,
            t.block_number
            ORDER BY
                bid_block_number DESC,
                bid_value DESC
        ) = 1
),
base_with_bids AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        buy_index,
        trace_index,
        event_index,
        platform_address,
        function_sig,
        function_name,
        event_type,
        segmented_data,
        nft_address,
        tokenid,
        punk_from_address,
        punk_to_address,
        bid_block_number,
        bid_tx_hash,
        bid_from_address,
        VALUE,
        accepted_bid_price,
        bid_value,
        bid_block_number,
        bid_tx_hash,
        bid_from_address,
        _log_id,
        _inserted_timestamp
    FROM
        base
        LEFT JOIN latest_punk_bids USING (
            tx_hash,
            buy_index
        )
),
nft_transfers AS (
    SELECT
        tx_hash AS transfers_tx_hash,
        event_index,
        from_address AS nft_from_address,
        to_address AS nft_to_address,
        contract_address AS nft_address,
        token_id AS tokenid
    FROM
        {{ ref('nft__ez_nft_transfers') }}
    WHERE
        block_timestamp :: DATE >= '2017-06-20'
        AND contract_address = '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                raw_traces
        ) qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            to_address,
            tokenid
            ORDER BY
                event_index DESC
        ) = 1

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
tx_data AS (
    SELECT
        tx_hash,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        origin_function_signature,
        tx_fee,
        input_data
    FROM
        {{ ref('core__fact_transactions') }}
    WHERE
        block_timestamp :: DATE >= '2017-06-20'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                raw_traces
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
        t.tx_hash,
        buy_index,
        trace_index,
        event_index,
        platform_address,
        function_sig,
        function_name,
        event_type,
        segmented_data,
        nft_address,
        tokenid,
        NULL AS erc1155_value,
        punk_from_address,
        punk_to_address,
        nft_to_address,
        punk_from_address AS seller_address,
        IFF(
            function_name = 'buyPunk',
            punk_to_address,
            nft_to_address
        ) AS buyer_address,
        VALUE,
        accepted_bid_price,
        bid_value,
        bid_block_number,
        bid_tx_hash,
        bid_from_address,
        'ETH' AS currency_address,
        'larva labs' AS platform_name,
        'cryptopunks' AS platform_exchange_version,
        CASE
            WHEN function_name = 'buyPunk' THEN VALUE
            WHEN function_name = 'acceptBidForPunk'
            AND bid_from_address = buyer_address THEN bid_value
            WHEN function_name = 'acceptBidForPunk'
            AND bid_from_address != buyer_address
            AND accepted_bid_price > 0 THEN accepted_bid_price
            ELSE bid_value
        END AS total_price_raw,
        0 AS total_fees_raw,
        0 AS platform_fee_raw,
        0 AS creator_fee_raw,
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
            t.tokenid,
            '-',
            _log_id,
            '-',
            platform_exchange_version
        ) AS nft_log_id
    FROM
        base_with_bids t
        INNER JOIN nft_transfers n
        ON t.tx_hash = n.transfers_tx_hash
        AND t.punk_from_address = n.nft_from_address
        AND t.tokenid = n.tokenid
        INNER JOIN tx_data USING (tx_hash)
)
SELECT
    *
FROM
    final_base qualify(ROW_NUMBER() over(PARTITION BY nft_log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
