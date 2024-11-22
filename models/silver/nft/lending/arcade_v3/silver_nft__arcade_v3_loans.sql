{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH raw_traces AS (

    SELECT
        *,
        decoded_data :function_name :: STRING AS function_name
    FROM
        {{ ref('silver__decoded_traces') }}
    WHERE
        block_timestamp :: DATE >= '2023-08-29'
        AND to_address IN (
            '0xb7bfcca7d7ff0f371867b770856fac184b185878',
            -- origination controller v3
            '0xe5b12befaf3a91065da7fdd461ded2d8f8ecb7be',
            -- borrower note v3
            '0xf764442856eb3fe68a0828e07246a4b395e800fa' -- fee controller
        )
        AND TYPE IN (
            'CALL',
            'STATICCALL'
        )
        AND function_name IN (
            'initializeLoan',
            'initializeLoanWithItems',
            'rolloverLoan',
            'rolloverLoanWithItems',
            'ownerOf',
            'getFeesRollover',
            'getFeesOrigination'
        )
        AND tx_status = 'SUCCESS'
        AND trace_status = 'SUCCESS'

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
initialize_loan AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        trace_index,
        decoded_data,
        function_name,
        decoded_data :decoded_input_data :borrower :: STRING AS borrower,
        decoded_data :decoded_input_data :lender :: STRING AS lender,
        decoded_data :decoded_input_data :loanTerms :affiliateCode :: STRING AS affiliate_code,
        decoded_data :decoded_input_data :loanTerms :collateralAddress :: STRING AS collateral_address,
        decoded_data :decoded_input_data :loanTerms :collateralId :: STRING AS collateral_id,
        TO_TIMESTAMP(
            decoded_data :decoded_input_data :loanTerms :deadline
        ) AS deadline,
        decoded_data :decoded_input_data :loanTerms :durationSecs :: INT AS duration_seconds,
        decoded_data :decoded_input_data :loanTerms :payableCurrency :: STRING AS loan_currency,
        decoded_data :decoded_input_data :loanTerms :principal :: INT AS principal,
        decoded_data :decoded_input_data :loanTerms :proratedInterestRate :: INT AS prorated_interest_rate,
        decoded_data :decoded_output_data :loanId :: STRING AS loanid,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) AS intra_tx_grouping
    FROM
        raw_traces
    WHERE
        function_name IN (
            'initializeLoan',
            'initializeLoanWithItems'
        )
        AND to_address = '0xb7bfcca7d7ff0f371867b770856fac184b185878'
),
rollover_loan AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        trace_index,
        decoded_data,
        function_name,
        decoded_data,
        decoded_data :decoded_input_data :lender :: STRING AS lender,
        decoded_data :decoded_input_data :loanTerms :affiliateCode :: STRING AS affiliate_code,
        decoded_data :decoded_input_data :loanTerms :collateralAddress :: STRING AS collateral_address,
        decoded_data :decoded_input_data :loanTerms :collateralId :: STRING AS collateral_id,
        TO_TIMESTAMP(
            decoded_data :decoded_input_data :loanTerms :deadline
        ) AS deadline,
        decoded_data :decoded_input_data :loanTerms :durationSecs :: INT AS duration_seconds,
        decoded_data :decoded_input_data :loanTerms :payableCurrency :: STRING AS loan_currency,
        decoded_data :decoded_input_data :loanTerms :principal :: INT AS principal,
        decoded_data :decoded_input_data :loanTerms :proratedInterestRate :: INT AS prorated_interest_rate,
        -- need to divide by 1e18 and it'll be in bps
        decoded_data :decoded_output_data :newLoanId :: STRING AS loanid,
        decoded_data :decoded_input_data :oldLoanId :: STRING AS old_loanid,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) AS intra_tx_grouping
    FROM
        raw_traces
    WHERE
        function_name IN (
            'rolloverLoan',
            'rolloverLoanWithItems'
        )
        AND to_address = '0xb7bfcca7d7ff0f371867b770856fac184b185878'
),
rollover_borrower AS (
    SELECT
        tx_hash,
        decoded_data :decoded_input_data :tokenId :: STRING AS old_loanid,
        -- or loanid
        decoded_data :decoded_output_data :output_1 :: STRING AS borrower,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) AS intra_tx_grouping
    FROM
        raw_traces
    WHERE
        to_address IN (
            '0xe5b12befaf3a91065da7fdd461ded2d8f8ecb7be' -- borrower note
        )
        AND TYPE IN (
            'STATICCALL'
        )
        AND function_name IN ('ownerOf')
        AND from_address = '0xb7bfcca7d7ff0f371867b770856fac184b185878' -- origination controller v3
),
rollover_loan_filled AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        trace_index,
        decoded_data,
        function_name,
        borrower,
        lender,
        affiliate_code,
        collateral_address,
        collateral_id,
        deadline,
        duration_seconds,
        loan_currency,
        principal,
        prorated_interest_rate,
        loanid,
        intra_tx_grouping
    FROM
        rollover_loan
        INNER JOIN rollover_borrower USING (
            tx_hash,
            old_loanid,
            intra_tx_grouping
        )
),
origination_fees AS (
    SELECT
        tx_hash,
        trace_index,
        decoded_data,
        function_name AS fee_function_name
    FROM
        raw_traces
    WHERE
        to_address IN (
            '0xf764442856eb3fe68a0828e07246a4b395e800fa'
        )
        AND from_address = '0xb7bfcca7d7ff0f371867b770856fac184b185878'
        AND TYPE IN (
            'STATICCALL'
        )
        AND function_name IN (
            'getFeesRollover',
            'getFeesOrigination'
        )
),
origination_fees_initialize AS (
    SELECT
        tx_hash,
        fee_function_name,
        decoded_data :decoded_output_data :output_1 :borrowerOriginationFee :: INT AS borrower_origination_fee,
        decoded_data :decoded_output_data :output_1 :lenderDefaultFee :: INT AS lender_default_fee,
        decoded_data :decoded_output_data :output_1 :lenderInterestFee :: INT AS lender_interest_fee,
        decoded_data :decoded_output_data :output_1 :lenderOriginationFee :: INT AS lender_origination_fee,
        decoded_data :decoded_output_data :output_1 :lenderPrincipalFee :: INT AS lender_principal_fee,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) AS intra_tx_grouping
    FROM
        origination_fees
    WHERE
        fee_function_name IN (
            'getFeesOrigination'
        )
),
origination_fees_rollover AS (
    SELECT
        tx_hash,
        fee_function_name,
        decoded_data :decoded_output_data :output_1 :borrowerRolloverFee :: INT AS borrower_rollover_fee,
        decoded_data :decoded_output_data :output_1 :lenderRolloverFee :: INT AS lender_rollover_fee,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) AS intra_tx_grouping
    FROM
        origination_fees
    WHERE
        fee_function_name IN (
            'getFeesRollover'
        )
),
initialize_with_fees AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        trace_index,
        decoded_data,
        function_name,
        borrower,
        lender,
        affiliate_code,
        collateral_address,
        collateral_id,
        deadline,
        duration_seconds,
        loan_currency,
        principal,
        prorated_interest_rate,
        loanid,
        intra_tx_grouping,
        fee_function_name,
        borrower_origination_fee,
        lender_default_fee,
        lender_interest_fee,
        lender_origination_fee,
        lender_principal_fee,
        NULL AS borrower_rollover_fee,
        NULL AS lender_rollover_fee
    FROM
        initialize_loan
        INNER JOIN origination_fees_initialize USING (
            tx_hash,
            intra_tx_grouping
        )
),
rollover_with_fees AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        trace_index,
        decoded_data,
        function_name,
        borrower,
        lender,
        affiliate_code,
        collateral_address,
        collateral_id,
        deadline,
        duration_seconds,
        loan_currency,
        principal,
        prorated_interest_rate,
        loanid,
        intra_tx_grouping,
        fee_function_name,
        NULL AS borrower_origination_fee,
        NULL AS lender_default_fee,
        NULL AS lender_interest_fee,
        NULL AS lender_origination_fee,
        NULL AS lender_principal_fee,
        borrower_rollover_fee,
        lender_rollover_fee
    FROM
        rollover_loan_filled
        INNER JOIN origination_fees_rollover USING (
            tx_hash,
            intra_tx_grouping
        )
),
combined AS (
    SELECT
        *
    FROM
        initialize_with_fees
    UNION ALL
    SELECT
        *
    FROM
        rollover_with_fees
),
logs AS (
    SELECT
        tx_hash,
        event_name,
        event_index,
        contract_address,
        decoded_flat,
        decoded_flat :loanId :: STRING AS loanid,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_timestamp :: DATE >= '2023-08-29'
        AND contract_address = LOWER('0x89bc08BA00f135d608bc335f6B33D7a9ABCC98aF') -- loan core v3
        AND event_name IN ('LoanStarted')

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
    event_name,
    trace_index,
    function_name,
    'arcade' AS platform_name,
    contract_address AS platform_address,
    'arcade v3' AS platform_exchange_version,
    borrower AS borrower_address,
    lender AS lender_address,
    loanid,
    collateral_address AS nft_address,
    collateral_id AS tokenid,
    principal AS principal_unadj,
    principal * (prorated_interest_rate / pow(10, 22)) AS interest_amount,
    interest_amount + principal AS debt_unadj,
    loan_currency AS loan_token_address,
    prorated_interest_rate / pow(
        10,
        20
    ) AS interest_rate_percentage,
    interest_rate_percentage / (
        duration_seconds / 86400
    ) * 365 AS annual_percentage_rate,
    borrower_origination_fee,
    lender_default_fee,
    lender_interest_fee,
    lender_origination_fee,
    lender_principal_fee,
    borrower_rollover_fee,
    lender_rollover_fee,
    (COALESCE(borrower_origination_fee, 0) + COALESCE(lender_default_fee, 0) + COALESCE(lender_interest_fee, 0) + COALESCE(lender_origination_fee, 0) + COALESCE(lender_principal_fee, 0) + COALESCE(borrower_rollover_fee, 0) + COALESCE(lender_rollover_fee, 0)) AS platform_fee_unadj,
    'new_loan' AS event_type,
    'fixed' AS loan_term_type,
    block_timestamp AS loan_start_timestamp,
    deadline AS deadline_loan_due_timestamp,
    duration_seconds AS loan_tenure,
    DATEADD(
        seconds,
        duration_seconds,
        loan_start_timestamp
    ) AS loan_due_timestamp,
    affiliate_code,
    intra_tx_grouping,
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
    combined
    INNER JOIN logs USING (
        tx_hash,
        loanid
    )
