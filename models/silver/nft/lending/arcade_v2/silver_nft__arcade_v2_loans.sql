{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['stale']
) }}

WITH raw_traces AS (

    SELECT
        *,
        decoded_data :function_name :: STRING AS function_name
    FROM
        {{ ref('silver__decoded_traces') }}
    WHERE
        block_timestamp :: DATE >= '2022-06-20'
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
            'ownerOf',
            'getOriginationFee',
            'getRolloverFee'
        )
        AND trace_status = 'SUCCESS'
        AND tx_status = 'SUCCESS'

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
new_loans AS (
    SELECT
        block_number,
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
        decoded_data :decoded_input_data :loanTerms :durationSecs :: INT AS DURATION,
        decoded_data :decoded_input_data :loanTerms :interestRate :: INT AS interest_rate,
        decoded_data :decoded_input_data :loanTerms :numInstallments :: INT AS num_installments,
        decoded_data :decoded_input_data :loanTerms :payableCurrency :: STRING AS loan_currency,
        decoded_data :decoded_input_data :loanTerms :principal :: INT AS principal,
        decoded_data :decoded_output_data :loanId :: STRING AS loanid
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
        block_number,
        block_timestamp,
        tx_hash,
        trace_index,
        from_address,
        to_address,
        decoded_data,
        function_name,
        decoded_data :decoded_input_data :lender :: STRING AS lender,
        decoded_data :decoded_input_data :loanTerms :collateralAddress :: STRING AS collateral_token_address,
        decoded_data :decoded_input_data :loanTerms :collateralId :: STRING AS collateral_tokenid,
        TO_TIMESTAMP(
            decoded_data :decoded_input_data :loanTerms :deadline
        ) AS deadline,
        decoded_data :decoded_input_data :loanTerms :durationSecs :: INT AS DURATION,
        decoded_data :decoded_input_data :loanTerms :interestRate :: INT AS interest_rate,
        decoded_data :decoded_input_data :loanTerms :numInstallments :: INT AS num_installments,
        decoded_data :decoded_input_data :loanTerms :payableCurrency :: STRING AS loan_currency,
        decoded_data :decoded_input_data :loanTerms :principal :: INT AS principal,
        decoded_data :decoded_input_data :oldLoanId :: STRING AS old_loanid,
        decoded_data :decoded_output_data :newLoanId :: STRING AS new_loanid
    FROM
        raw_traces
    WHERE
        function_name IN (
            'rolloverLoan',
            'rolloverLoanWithItems'
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
        block_number,
        block_timestamp,
        tx_hash,
        from_address,
        decoded_data :decoded_input_data :tokenId :: STRING AS old_loanid,
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
        block_number,
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
),
logs AS (
    SELECT
        tx_hash,
        event_index,
        event_name,
        contract_address,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_timestamp :: DATE >= '2022-06-20'
        AND contract_address IN (
            '0x81b2f8fc75bab64a6b144aa6d2faa127b4fa7fd9' -- loan core proxy
        )
        AND event_name IN (
            'LoanStarted'
        )

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
    'arcade' AS platform_name,
    contract_address AS platform_address,
    'arcade v2' AS platform_exchange_version,
    trace_index,
    from_address,
    to_address,
    decoded_data,
    function_name,
    borrower AS borrower_address,
    lender AS lender_address,
    loanid,
    collateral_token_address AS nft_address,
    collateral_tokenid AS tokenId,
    principal AS principal_unadj,
    interest_rate / pow(
        10,
        22
    ) * principal AS interest_raw,
    principal + interest_raw AS debt_unadj,
    loan_currency AS loan_token_address,
    interest_rate / pow(
        10,
        20
    ) AS interest_rate_percentage,
    interest_rate_percentage / (
        DURATION / 86400
    ) * 365 AS annual_percentage_rate,
    fee_function_name,
    num_installments,
    principal * (origination_fee_bps / pow(10, 4)) AS origination_fee,
    origination_fee AS platform_fee_unadj,
    'new_loan' AS event_type,
    'fixed' AS loan_term_type,
    block_timestamp AS loan_start_timestamp,
    DATEADD(
        seconds,
        DURATION,
        loan_start_timestamp
    ) AS loan_due_timestamp,
    deadline AS deadline_loan_due_timestamp,
    DURATION AS loan_tenure,
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
    INNER JOIN logs l USING (
        tx_hash
    )
    LEFT JOIN origination o USING (tx_hash)
