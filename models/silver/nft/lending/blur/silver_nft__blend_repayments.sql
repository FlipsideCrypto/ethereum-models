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
repay_txs_rn AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS intra_tx_grouping
    FROM
        repay_txs
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
        utils.udf_hex_to_int(output) AS debt_unadj,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS principal_unadj,
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
        {{ ref('core__fact_traces') }}
    WHERE
        block_timestamp >= '2023-05-01'
        AND TYPE = 'DELEGATECALL'
        AND trace_status = 'SUCCESS'
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
traces_raw_rn AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) AS intra_tx_grouping
    FROM
        traces_raw
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
        debt_unadj,
        principal_unadj,
        interest_rate_bps,
        loan_start_timestamp,
        loan_paid_timestamp,
        _log_id,
        _inserted_timestamp
    FROM
        traces_raw_rn
        INNER JOIN repay_txs_rn USING (
            tx_hash,
            intra_tx_grouping
        )
),
loan_fill AS (
    SELECT
        *,
        LAG(
            lender_address
        ) ignore nulls over (
            PARTITION BY lienid
            ORDER BY
                block_timestamp ASC
        ) AS prev_lender_address
    FROM
        {{ ref('silver_nft__blend_loans') }}
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
        t.debt_unadj,
        t.principal_unadj,
        t.interest_rate_bps,
        t.loan_start_timestamp,
        t.loan_paid_timestamp,
        t._log_id,
        t._inserted_timestamp
    FROM
        traces_base t
        INNER JOIN loan_fill b USING (
            tx_hash,
            lienId
        )
    WHERE
        t.event_name = 'Refinance'
        AND b.event_name = 'Refinance'
),
loan_details AS (
    SELECT
        lienid,
        borrower_address,
        lender_address,
        nft_address,
        tokenId
    FROM
        {{ ref('silver_nft__blend_loans') }}
        qualify ROW_NUMBER() over (
            PARTITION BY lienid
            ORDER BY
                block_timestamp DESC
        ) = 1
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
        b.lender_address,
        t.debt_unadj,
        t.principal_unadj,
        t.interest_rate_bps,
        t.loan_start_timestamp,
        t.loan_paid_timestamp,
        t._log_id,
        t._inserted_timestamp
    FROM
        traces_base t
        INNER JOIN loan_details b
        ON t.lienid = b.lienid
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
    IFF(
        event_name = 'Repay',
        'repay',
        'refinance'
    ) AS event_type,
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
    debt_unadj :: INT AS debt_unadj,
    principal_unadj :: INT AS principal_unadj,
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
    interest_rate_percentage AS annual_percentage_rate,
    'perpetual' AS loan_term_type,
    loan_start_timestamp,
    loan_paid_timestamp,
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
    final_base
