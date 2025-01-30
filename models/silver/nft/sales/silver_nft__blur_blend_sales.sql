{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH raw_traces AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        trace_index,
        from_address,
        to_address,
        input,
        LEFT(
            input,
            10
        ) AS function_sig,
        regexp_substr_all(SUBSTR(input, 11), '.{64}') AS segmented_data
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        block_timestamp >= '2023-05-01'
        AND from_address = '0x29469395eaf6f95920e59f858042f0e28d98a20b'
        AND to_address IN (
            '0x97bdb4aed0b50a335a78ed24f68528ce3222af72',
            -- old blend contracts in asc
            '0x13244ef110692c1d8256c8dd4aa0a09bb5af0156',
            '0xb258ca5559b11cd702f363796522b04d7722ea56' -- latest
        )
        AND TYPE = 'DELEGATECALL'
        AND trace_succeeded
        AND function_sig IN (
            '0xe7efc178',
            -- buyLocked
            '0x8553b234',
            --buyLockedEth
            '0xb2a0bb86',
            -- buyToBorrowLockedEth
            '0x2e2fb18b' --buyToBorrowLocked
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
buy_locked AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        trace_index,
        from_address,
        to_address,
        input,
        function_sig,
        regexp_substr_all(SUBSTR(input, 11), '.{64}') AS segmented_data,
        '0x' || SUBSTR(
            segmented_data [0] :: STRING,
            25
        ) AS lender,
        '0x' || SUBSTR(
            segmented_data [1] :: STRING,
            25
        ) AS borrower,
        '0x' || SUBSTR(
            segmented_data [2] :: STRING,
            25
        ) AS nft_address,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: STRING AS tokenId,
        utils.udf_hex_to_int(
            segmented_data [4] :: STRING
        ) :: INT AS borrowed_amount,
        utils.udf_hex_to_int(
            segmented_data [9] :: STRING
        ) :: INT / 32 AS offer_start_index,
        '0x' || SUBSTR(
            segmented_data [offer_start_index] :: STRING,
            25
        ) AS offer_info_borrower,
        utils.udf_hex_to_int(
            segmented_data [offer_start_index + 1] :: STRING
        ) :: STRING AS lienId,
        utils.udf_hex_to_int(
            segmented_data [offer_start_index + 2] :: STRING
        ) :: INT AS price_raw,
        utils.udf_hex_to_int(
            segmented_data [offer_start_index + 6] :: STRING
        ) :: INT / 32 AS fee_array_index,
        utils.udf_hex_to_int(
            segmented_data [offer_start_index + fee_array_index] :: STRING
        ) :: INT / pow(
            10,
            4
        ) AS fee_rate,
        price_raw * fee_rate AS fees_raw,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            lienId
            ORDER BY
                trace_index ASC
        ) AS intra_tx_grouping
    FROM
        raw_traces
    WHERE
        function_sig IN (
            '0xe7efc178',
            -- buyLocked
            '0x8553b234' --buyLockedEth
        )
),
buy_to_borrow AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        trace_index,
        from_address,
        to_address,
        input,
        LEFT(
            input,
            10
        ) AS function_sig,
        regexp_substr_all(SUBSTR(input, 11), '.{64}') AS segmented_data,
        '0x' || SUBSTR(
            segmented_data [0] :: STRING,
            25
        ) AS lender,
        '0x' || SUBSTR(
            segmented_data [1] :: STRING,
            25
        ) AS borrower,
        '0x' || SUBSTR(
            segmented_data [2] :: STRING,
            25
        ) AS nft_address,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: STRING AS tokenId,
        utils.udf_hex_to_int(
            segmented_data [4] :: STRING
        ) :: INT AS borrowed_amount,
        utils.udf_hex_to_int(
            segmented_data [9] :: STRING
        ) :: INT / 32 AS sellinput_start_index,
        utils.udf_hex_to_int(
            segmented_data [10] :: STRING
        ) :: INT / 32 AS loaninput_start_index,
        utils.udf_hex_to_int(
            segmented_data [sellinput_start_index] :: STRING
        ) :: INT / 32 + (
            sellinput_start_index
        ) AS sellinput_index,
        '0x' || SUBSTR(
            segmented_data [sellinput_index] :: STRING,
            25
        ) AS sellinput_borrower,
        -- seller of locked nft
        utils.udf_hex_to_int(
            segmented_data [sellinput_index + 1] :: STRING
        ) :: STRING AS lienId,
        utils.udf_hex_to_int(
            segmented_data [sellinput_index + 2] :: STRING
        ) :: INT AS price_raw,
        utils.udf_hex_to_int(
            segmented_data [sellinput_index + 6] :: STRING
        ) :: INT / 32 AS fee_array_index,
        utils.udf_hex_to_int(
            segmented_data [sellinput_index + fee_array_index] :: STRING
        ) :: INT / pow(
            10,
            4
        ) AS fee_rate,
        price_raw * fee_rate AS fees_raw,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            lienId
            ORDER BY
                trace_index ASC
        ) AS intra_tx_grouping
    FROM
        raw_traces
    WHERE
        function_sig IN (
            '0xb2a0bb86',
            -- buyToBorrowLockedEth
            '0x2e2fb18b' --buyToBorrowLocked
        )
),
traces_combined AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        trace_index,
        from_address,
        to_address,
        input,
        function_sig,
        segmented_data,
        lender,
        borrower,
        nft_address,
        tokenId,
        borrowed_amount,
        offer_info_borrower AS current_nft_owner,
        lienId,
        price_raw,
        fee_rate,
        fees_raw,
        intra_tx_grouping
    FROM
        buy_locked
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        trace_index,
        from_address,
        to_address,
        input,
        function_sig,
        segmented_data,
        lender,
        borrower,
        nft_address,
        tokenId,
        borrowed_amount,
        sellinput_borrower AS current_nft_owner,
        lienId,
        price_raw,
        fee_rate,
        fees_raw,
        intra_tx_grouping
    FROM
        buy_to_borrow
),
raw_logs AS (
    SELECT
        tx_hash,
        event_index,
        event_name,
        contract_address,
        decoded_log AS decoded_flat,
        decoded_flat :lienId :: STRING AS logs_lienId,
        decoded_flat :collection :: STRING AS logs_nft_address,
        decoded_flat :tokenId :: STRING AS logs_tokenId,
        decoded_flat :buyer :: STRING AS buyer_address,
        decoded_flat :seller :: STRING AS seller_address,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            logs_lienId
            ORDER BY
                event_index ASC
        ) AS intra_tx_grouping,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        block_timestamp >= '2023-05-01'
        AND contract_address = '0x29469395eaf6f95920e59f858042f0e28d98a20b'
        AND event_name IN (
            'BuyLocked'
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
base AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        intra_tx_grouping,
        event_index,
        trace_index,
        from_address,
        to_address,
        input,
        function_sig,
        segmented_data,
        lender,
        borrower,
        nft_address,
        tokenId,
        borrowed_amount,
        current_nft_owner,
        lienId,
        price_raw,
        fee_rate,
        fees_raw,
        event_name,
        decoded_flat,
        logs_lienId,
        logs_nft_address,
        logs_tokenId,
        buyer_address,
        seller_address,
        _log_id,
        _inserted_timestamp
    FROM
        traces_combined
        JOIN raw_logs USING (
            tx_hash,
            intra_tx_grouping
        )
),
tx_data AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_number,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        origin_function_signature,
        tx_fee,
        input_data
    FROM
        {{ ref('core__fact_transactions') }}
    WHERE
        block_timestamp :: DATE >= '2023-05-01'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                base
        )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    intra_tx_grouping,
    event_index,
    trace_index,
    from_address,
    to_address,
    'sale' AS event_type,
    '0x29469395eaf6f95920e59f858042f0e28d98a20b' AS platform_address,
    'blur' AS platform_name,
    'blend' AS platform_exchange_version,
    -- current blend (v1) uses 0xb258ca5559b11cd702f363796522b04d7722ea56 as proxy
    function_sig,
    CASE
        WHEN function_sig = '0x8553b234' THEN 'buyLockedETH'
        WHEN function_sig = '0xe7efc178' THEN 'buyLocked'
        WHEN function_sig = '0xb2a0bb86' THEN 'buyToBorrowLockedEth'
        WHEN function_sig = '0x2e2fb18b' THEN 'buyToBorrowLocked'
    END AS function_name,
    seller_address,
    buyer_address,
    nft_address,
    NULL AS erc1155_value,
    tokenId,
    '0x0000000000a39bb272e79075ade125fd351887ac' AS currency_address,
    price_raw AS total_price_raw,
    fees_raw AS total_fees_raw,
    fees_raw AS creator_fee_raw,
    0 AS platform_fee_raw,
    fee_rate,
    lender,
    borrower,
    borrowed_amount,
    current_nft_owner,
    lienId,
    logs_lienId,
    logs_nft_address,
    logs_tokenId,
    tx_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
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
    base
    INNER JOIN tx_data USING (tx_hash) qualify(ROW_NUMBER() over(PARTITION BY nft_log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
