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
        ) AS lienId,
        utils.udf_hex_to_int(
            segmented_data [offer_start_index + 2] :: STRING
        ) :: INT AS price,
        utils.udf_hex_to_int(
            segmented_data [offer_start_index + 6] :: STRING
        ) :: INT / 32 AS fee_array_index,
        utils.udf_hex_to_int(
            segmented_data [offer_start_index + fee_array_index] :: STRING
        ) :: INT AS fees,
        price * fees AS fees_raw,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            lienId
            ORDER BY
                trace_index ASC
        ) AS intra_tx_grouping
    FROM
        {{ ref('silver__traces') }}
    WHERE
        block_timestamp >= '2023-05-01'
        AND from_address = '0x29469395eaf6f95920e59f858042f0e28d98a20b'
        AND to_address = '0xb258ca5559b11cd702f363796522b04d7722ea56'
        AND TYPE = 'DELEGATECALL'
        AND trace_status = 'SUCCESS'
        AND function_sig IN (
            '0xe7efc178',
            -- buyLocked
            '0x8553b234' --buyLockedEth
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
raw_logs AS (
    SELECT
        tx_hash,
        event_index,
        event_name,
        contract_address,
        decoded_flat,
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
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_timestamp >= '2023-05-01'
        AND contract_address = '0x29469395eaf6f95920e59f858042f0e28d98a20b'
        AND event_name IN (
            'BuyLocked'
        )
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '24 hours'
    FROM
        {{ this }}
)
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
        offer_start_index,
        offer_info_borrower,
        lienId,
        price,
        fee_array_index,
        fees,
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
        raw_traces
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
        {{ ref('silver__transactions') }}
    WHERE
        block_timestamp :: DATE >= '2023-05-01'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                base
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
    'sale' AS event_type,
    '0x29469395eaf6f95920e59f858042f0e28d98a20b' AS platform_address,
    'blur' AS platform_name,
    'blend' AS platform_exchange_version,
    -- current blend (v1) uses 0xb258ca5559b11cd702f363796522b04d7722ea56 as proxy
    function_sig,
    IFF(
        function_sig = '0x8553b234',
        'buyLockedETH',
        'buyLocked'
    ) AS function_name,
    seller_address,
    buyer_address,
    nft_address,
    NULL AS erc1155_value,
    tokenId,
    '0x0000000000a39bb272e79075ade125fd351887ac' AS currency_address,
    price AS total_price_raw,
    fees_raw AS total_fees_raw,
    fees_raw AS creator_fee_raw,
    0 AS platform_fee_raw,
    fee_array_index,
    fees,
    lender,
    borrower,
    borrowed_amount,
    offer_info_borrower,
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
