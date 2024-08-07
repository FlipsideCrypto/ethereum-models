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
        block_timestamp :: DATE >= '2022-06-20' --and tx_hash = '0xda999f11ae4a304ad230faff9ed124ff409ca906fab9da2ab8defb6552b4d79d' -- v2.1, new structure altogether
        AND to_address IN (
            '0x2df5c801f2f082287241c8cb7f3d517c3cba2620',
            -- origination controller implementation v2
            '0xaef68c90057886a1d3f590d0cfee0597e4a89f35',
            -- origination controller implementation v2.1
            '0x337104a4f06260ff327d6734c555a0f5d8f863aa',
            -- borrower note
            '0x41e538817c3311ed032653bee5487a113f8cff9f' -- fee controller
        )
        AND TYPE IN (
            'DELEGATECALL',
            'STATICCALL'
        )
        AND function_name IN (
            'initializeLoan',
            'initializeLoanWithItems',
            'rolloverLoan',
            'rolloverLoanWithItems',
            -- essentially a new loan since the old loan is repaid
            'ownerOf',
            'getOriginationFee',
            'getRolloverFee'
        )
        AND trace_status = 'SUCCESS'
        AND tx_status = 'SUCCESS'
),
new_loans AS (
    SELECT
        block_timestamp,
        tx_hash,
        trace_index,
        from_address,
        to_address,
        decoded_data,
        function_name,
        decoded_data :decoded_input_data :borrower :: STRING AS borrower,
        decoded_data :decoded_input_data :lender :: STRING AS lender,
        decoded_data :decoded_input_data :loanTerms :collateralAddress :: STRING AS collateral_token_address,
        decoded_data :decoded_input_data :loanTerms :collateralId :: STRING AS collateral_tokenid,
        TO_TIMESTAMP(
            decoded_data :decoded_input_data :loanTerms :deadline
        ) AS deadline,
        -- exact timestamp
        decoded_data :decoded_input_data :loanTerms :durationSecs AS DURATION,
        decoded_data :decoded_input_data :loanTerms :interestRate :: INT AS interest_rate,
        -- needs to be divided by 1e18, then that becomes basis points. if results = 400, then it's 400 bp = 4%
        decoded_data :decoded_input_data :loanTerms :numInstallments :: INT AS num_installments,
        -- can either be 0 or > 2
        decoded_data :decoded_input_data :loanTerms :payableCurrency :: STRING AS loan_currency,
        decoded_data :decoded_input_data :loanTerms :principal :: INT AS principal,
        decoded_data :decoded_output_data :loanId :: INT AS loanid
    FROM
        raw_traces
    WHERE
        function_name IN (
            'initializeLoan',
            'initializeLoanWithItems'
        )
        AND TYPE = 'DELEGATECALL'
        AND to_address IN (
            '0x2df5c801f2f082287241c8cb7f3d517c3cba2620',
            -- origination controller implementation v2
            '0xaef68c90057886a1d3f590d0cfee0597e4a89f35' -- origination controller implementation v2.1
        )
),
rollover_loans AS (
    SELECT
        block_timestamp,
        tx_hash,
        trace_index,
        from_address,
        to_address,
        decoded_data,
        function_name,
        --decoded_data:decoded_input_data:borrower::string as borrower,
        decoded_data :decoded_input_data :lender :: STRING AS lender,
        decoded_data :decoded_input_data :loanTerms :collateralAddress :: STRING AS collateral_token_address,
        decoded_data :decoded_input_data :loanTerms :collateralId :: STRING AS collateral_tokenid,
        TO_TIMESTAMP(
            decoded_data :decoded_input_data :loanTerms :deadline
        ) AS deadline,
        -- exact timestamp
        decoded_data :decoded_input_data :loanTerms :durationSecs AS DURATION,
        decoded_data :decoded_input_data :loanTerms :interestRate :: INT AS interest_rate,
        -- needs to be divided by 1e18, then that becomes basis points. if results = 400, then it's 400 bp = 4%
        decoded_data :decoded_input_data :loanTerms :numInstallments :: INT AS num_installments,
        -- can either be 0 or > 2
        decoded_data :decoded_input_data :loanTerms :payableCurrency :: STRING AS loan_currency,
        decoded_data :decoded_input_data :loanTerms :principal :: INT AS principal,
        decoded_data :decoded_input_data :oldLoanId :: INT AS old_loanid,
        decoded_data :decoded_output_data :newLoanId :: INT AS new_loanid
    FROM
        raw_traces
    WHERE
        function_name IN (
            'rolloverLoan',
            'rolloverLoanWithItems' -- essentially a new loan since the old loan is repaid
        )
        AND TYPE = 'DELEGATECALL'
        AND to_address IN (
            '0x2df5c801f2f082287241c8cb7f3d517c3cba2620',
            -- origination controller implementation v2
            '0xaef68c90057886a1d3f590d0cfee0597e4a89f35' -- origination controller implementation v2.1
        )
),
rollover_borrower AS (
    SELECT
        block_timestamp,
        tx_hash,
        from_address,
        decoded_data :decoded_input_data :tokenId :: INT AS old_loanid,
        -- or loanid
        decoded_data :decoded_output_data :output_1 :: STRING AS borrower
    FROM
        raw_traces
    WHERE
        to_address IN (
            '0x337104a4f06260ff327d6734c555a0f5d8f863aa' -- borrower note
        )
        AND TYPE IN (
            'STATICCALL'
        )
        AND function_name IN ('ownerOf')
        AND from_address = '0x4c52ca29388a8a854095fd2beb83191d68dc840b' -- origination controller proxy
),
rollover_loans_filled AS (
    SELECT
        block_timestamp,
        tx_hash,
        trace_index,
        from_address,
        to_address,
        decoded_data,
        function_name,
        borrower,
        lender,
        collateral_token_address,
        collateral_tokenid,
        deadline,
        -- exact timestamp
        DURATION,
        interest_rate,
        -- needs to be divided by 1e18, then that becomes basis points. if results = 400, then it's 400 bp = 4%
        num_installments,
        -- can either be 0 or > 2
        loan_currency,
        principal,
        new_loanid AS loanid
    FROM
        rollover_loans
        INNER JOIN rollover_borrower USING (
            tx_hash,
            old_loanid
        )
),
combined AS (
    SELECT
        *
    FROM
        new_loans
    UNION ALL
    SELECT
        *
    FROM
        rollover_loans_filled
),
origination AS (
    SELECT
        tx_hash,
        function_name AS fee_function_name,
        decoded_data :decoded_output_data :output_1 :: INT AS origination_fee_bps
    FROM
        raw_traces
    WHERE
        to_address IN (
            '0x41e538817c3311ed032653bee5487a113f8cff9f'
        )
        AND TYPE IN (
            'STATICCALL'
        )
        AND function_name IN (
            'getOriginationFee',
            'getRolloverFee'
        )
        AND trace_status = 'SUCCESS'
        AND tx_status = 'SUCCESS' qualify ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) = 1
)
SELECT
    block_timestamp,
    tx_hash,
    trace_index,
    from_address,
    to_address,
    decoded_data,
    function_name,
    borrower,
    lender,
    collateral_token_address,
    collateral_tokenid,
    deadline,
    DURATION,
    interest_rate,
    num_installments,
    loan_currency,
    principal,
    loanid,
    interest_rate / pow(
        10,
        20
    ) * principal AS interest_raw,
    fee_function_name,
    origination_fee_bps
FROM
    combined
    LEFT JOIN origination o USING (tx_hash)
