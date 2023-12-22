{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH calculation_helpers AS (

    SELECT
        *
    FROM
        (
            VALUES
                ('0x5c55cd67a6bd0d4c315b50cb6cd589bfb080017e'),
                ('0xacd3d255b93f6f91dd6311ae9a30cb256e118cf2'),
                ('0x5c06da8ec7945c05304d79895046d01a7943826d')
        ) t (helpers)
),
repay_txs AS (
    SELECT
        tx_hash,
        event_index,
        contract_address,
        event_name,
        decoded_flat :lienId AS lienId,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_timestamp :: DATE >= '2023-05-01'
        AND contract_address = '0x29469395eaf6f95920e59f858042f0e28d98a20b'
        AND event_name IN (
            'Repay',
            'Refinance'
        )
),
traces_raw AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        trace_index,
        from_address,
        to_address,
        input,
        output,
        LEFT(
            input,
            10
        ) AS function_sig,
        regexp_substr_all(SUBSTR(input, 11), '.{64}') AS segmented_data,
        segmented_data [0],
        utils.udf_hex_to_int(output) AS total_debt_unadj,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS principal_amount_unadj,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) AS interest_rate_bps,
        TO_TIMESTAMP(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS loan_start_timestamp,
        block_timestamp AS loan_paid_timestamp
    FROM
        {{ ref('silver__traces') }}
    WHERE
        block_timestamp >= '2023-05-01'
        AND TYPE = 'DELEGATECALL'
        AND trace_status = 'SUCCESS'
        AND tx_status = 'SUCCESS'
        AND from_address = '0x29469395eaf6f95920e59f858042f0e28d98a20b'
        AND to_address IN (
            SELECT
                helpers
            FROM
                calculation_helpers
        )
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                repay_txs
        )
        AND LEFT(
            input,
            10
        ) = '0x1b70b278' --computeCurrentDebt
),
traces_base AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        trace_index,
        event_index,
        from_address,
        to_address,
        function_sig,
        contract_address,
        event_name,
        lienId,
        total_debt_unadj,
        principal_amount_unadj,
        interest_rate_bps,
        loan_start_timestamp,
        loan_paid_timestamp,
        _log_id,
        _inserted_timestamp
    FROM
        traces_raw
        INNER JOIN repay_txs USING (tx_hash)
),
refinance_base AS (
    SELECT
        t.block_number,
        t.block_timestamp,
        t.tx_hash,
        t.trace_index,
        t.event_index,
        t.function_sig,
        t.contract_address,
        t.event_name,
        t.lienId,
        b.nft_address,
        b.tokenId,
        b.borrower_address,
        b.prev_lender_address AS lender_address,
        t.total_debt_unadj,
        t.principal_amount_unadj,
        t.interest_rate_bps,
        t.loan_start_timestamp,
        t.loan_paid_timestamp,
        b.prev_block_timestamp,
        t._log_id,
        t._inserted_timestamp
    FROM
        traces_base t
        INNER JOIN {{ ref('silver_nft__blend_loans_taken') }}
        b USING (
            tx_hash,
            lienId
        )
    WHERE
        t.event_name = 'Refinance'
        AND b.event_name = 'Refinance'
),
repay_base AS (
    SELECT
        t.block_number,
        t.block_timestamp,
        t.tx_hash,
        t.trace_index,
        t.event_index,
        t.function_sig,
        t.contract_address,
        t.event_name,
        t.lienId,
        b.nft_address,
        b.tokenId,
        b.borrower_address,
        IFF(
            prev_block_timestamp IS NULL,
            lender_address,
            b.prev_lender_address
        ) AS lender_address,
        t.total_debt_unadj,
        t.principal_amount_unadj,
        t.interest_rate_bps,
        t.loan_start_timestamp,
        t.loan_paid_timestamp,
        b.prev_block_timestamp,
        t._log_id,
        t._inserted_timestamp
    FROM
        traces_base t
        INNER JOIN {{ ref('silver_nft__blend_loans_taken') }}
        b
        ON t.lienid = b.lienid
        AND (
            (
                b.prev_block_timestamp IS NULL
                AND t.block_timestamp > b.block_timestamp
            )
            OR (
                t.block_timestamp > b.block_timestamp
                AND b.prev_block_timestamp > t.block_timestamp
            )
        )
    WHERE
        t.event_name = 'Repay'
),
final_base AS (
    SELECT
        *
    FROM
        refinance_base
    UNION ALL
    SELECT
        *
    FROM
        repay_base
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    trace_index,
    event_index,
    function_sig,
    contract_address,
    event_name,
    'blur' AS platform_name,
    '0x29469395eaf6f95920e59f858042f0e28d98a20b' AS platform_address,
    'blend v1' AS platform_exchange_version,
    lienId :: STRING AS lienId,
    lienId :: STRING AS loanId,
    nft_address,
    tokenId,
    borrower_address,
    lender_address,
    '0x0000000000a39bb272e79075ade125fd351887ac' AS loan_token_address,
    total_debt_unadj,
    principal_amount_unadj,
    0 AS platform_fee_unadj,
    interest_rate_bps,
    interest_rate_bps / pow(
        10,
        2
    ) AS interest_rate_percentage,
    interest_rate_bps / pow(
        10,
        4
    ) AS interest_rate,
    loan_start_timestamp,
    loan_paid_timestamp,
    prev_block_timestamp,
    _log_id,
    _inserted_timestamp,
    CONCAT(
        loanid,
        '-',
        _log_id
    ) AS nft_lending_id,
    -- unique identifer for each row
    {{ dbt_utils.generate_surrogate_key(
        ['loanid', 'borrower_address', 'lender_address', 'nft_address','tokenId','platform_exchange_version']
    ) }} AS unique_loan_id -- unique loan id across all lending tables
FROM
    final_base
