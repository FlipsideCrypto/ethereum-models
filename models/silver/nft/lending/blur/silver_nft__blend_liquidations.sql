{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH raw_traces AS (

    SELECT
        tx_hash,
        from_address,
        to_address,
        trace_index,
        LEFT(
            input,
            10
        ) AS function_sig,
        regexp_substr_all(SUBSTR(input, 11, len(input)), '.{64}') AS segmented_data,
        (utils.udf_hex_to_int(segmented_data [0] :: STRING) / 32) :: INT AS buffer,
        utils.udf_hex_to_int(
            segmented_data [buffer] :: STRING
        ) AS seize_count
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        block_timestamp :: DATE >= '2023-05-01'
        AND TYPE = 'DELEGATECALL'
        AND trace_succeeded
        AND tx_succeeded
        AND from_address = '0x29469395eaf6f95920e59f858042f0e28d98a20b'
        AND to_address IN (
            '0x97bdb4aed0b50a335a78ed24f68528ce3222af72',
            -- old blend contracts in asc
            '0x13244ef110692c1d8256c8dd4aa0a09bb5af0156',
            '0xb258ca5559b11cd702f363796522b04d7722ea56' -- latest
        )
        AND function_sig = '0x5b43226f'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
grouping_raw AS (
    SELECT
        *,
        INDEX - buffer - 1 AS new_index,
        TRUNC(
            new_index / 10
        ) AS intra_tx_grouping
    FROM
        raw_traces,
        LATERAL FLATTEN (
            input => segmented_data
        )
    WHERE
        INDEX >= (
            buffer + 1
        )
),
grouping_agg AS (
    SELECT
        tx_hash,
        from_address,
        to_address,
        trace_index,
        function_sig,
        buffer,
        seize_count,
        intra_tx_grouping,
        ARRAY_AGG(VALUE) within GROUP (
            ORDER BY
                new_index ASC,
                intra_tx_grouping ASC
        ) AS segmented_data_grouped
    FROM
        grouping_raw
    GROUP BY
        ALL
),
traces_base AS (
    SELECT
        *,
        '0x' || SUBSTR(
            segmented_data_grouped [0] :: STRING,
            25
        ) AS lender_address,
        '0x' || SUBSTR(
            segmented_data_grouped [1] :: STRING,
            25
        ) AS borrower_address,
        '0x' || SUBSTR(
            segmented_data_grouped [2] :: STRING,
            25
        ) AS nft_address,
        utils.udf_hex_to_int(
            segmented_data_grouped [3] :: STRING
        ) :: STRING AS tokenId,
        utils.udf_hex_to_int(
            segmented_data_grouped [4] :: STRING
        ) :: INT AS principal_unadj,
        TO_TIMESTAMP(
            utils.udf_hex_to_int(
                segmented_data_grouped [5] :: STRING
            )
        ) AS loan_start_timestamp,
        utils.udf_hex_to_int(
            segmented_data_grouped [6] :: STRING
        ) :: INT AS interest_rate_bps,
        utils.udf_hex_to_int(
            segmented_data_grouped [7] :: STRING
        ) AS auction_start_block,
        utils.udf_hex_to_int(
            segmented_data_grouped [8] :: STRING
        ) AS auction_duration,
        utils.udf_hex_to_int(
            segmented_data_grouped [9] :: STRING
        ) :: STRING AS lienId,
        utils.udf_hex_to_int(
            segmented_data_grouped [9] :: STRING
        ) :: STRING AS loanId
    FROM
        grouping_agg
),
raw_logs AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        contract_address,
        decoded_log AS decoded_flat,
        decoded_flat :collection :: STRING AS collection_address,
        decoded_flat :lienId AS lienId,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2023-05-01'
        AND contract_address = '0x29469395eaf6f95920e59f858042f0e28d98a20b'
        AND event_name = 'Seize'

{% if is_incremental() %}
AND _inserted_timestamp >= (
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
    event_index,
    event_name,
    'blur' AS platform_name,
    '0x29469395eaf6f95920e59f858042f0e28d98a20b' AS platform_address,
    'blend v1' AS platform_exchange_version,
    contract_address,
    decoded_flat,
    lienId,
    from_address,
    to_address,
    trace_index,
    function_sig,
    seize_count,
    intra_tx_grouping,
    lender_address,
    borrower_address,
    nft_address,
    tokenId,
    principal_unadj,
    '0x0000000000a39bb272e79075ade125fd351887ac' AS loan_token_address,
    interest_rate_bps,
    interest_rate_bps / pow(
        10,
        4
    ) AS interest_rate,
    interest_rate_bps / pow(
        10,
        2
    ) AS interest_rate_percentage,
    interest_rate_percentage AS annual_percentage_rate,
    loan_start_timestamp,
    auction_start_block,
    auction_duration,
    loanId,
    _log_id,
    _inserted_timestamp,
    CONCAT(
        loanid,
        '-',
        _log_id
    ) AS nft_lending_id,
    {{ dbt_utils.generate_surrogate_key(
        ['loanid', 'borrower_address', 'lender_address', 'nft_address','tokenId','platform_exchange_version']
    ) }} AS unique_loan_id
FROM
    raw_logs
    INNER JOIN traces_base USING (
        tx_hash,
        lienId
    )
