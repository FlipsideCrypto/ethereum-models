{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH raw_logs AS (

    SELECT
        *
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_timestamp >= '2022-03-01'
        AND contract_address IN (
            LOWER('0xd0a40eB7FD94eE97102BA8e9342243A2b2E22207'),
            LOWER('0x8252Df1d8b29057d1Afe3062bf5a64D503152BC8'),
            LOWER('0xf896527c49b44aAb3Cf22aE356Fa3AF8E331F280'),
            LOWER('0xD0C6e59B50C32530C627107F50Acc71958C4341F'),
            LOWER('0xe52cec0e90115abeb3304baa36bc2655731f7934')
        )
        AND event_name IN (
            'LoanStarted',
            'LoanRenegotiated'
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
loan_started AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        decoded_flat,
        contract_address,
        decoded_flat :borrower :: STRING AS borrower_address,
        decoded_flat :lender :: STRING AS lender_address,
        decoded_flat :loanExtras :referralFeeInBasisPoints AS referral_fee_bps,
        decoded_flat :loanExtras :revenueShareInBasisPoints AS revenue_share_bps,
        decoded_flat :loanExtras :revenueSharePartner :: STRING AS revenue_share_address,
        decoded_flat :loanId AS loanid,
        decoded_flat :loanTerms :borrower :: STRING AS loanterms_borrower_address,
        decoded_flat :loanTerms :loanAdminFeeInBasisPoints AS admin_fee_bps,
        decoded_flat :loanTerms :loanDuration AS loan_duration,
        decoded_flat :loanTerms :loanERC20Denomination :: STRING AS loan_denomination,
        decoded_flat :loanTerms :loanInterestRateForDurationInBasisPoints AS loanInterestRateForDurationInBasisPoints,
        decoded_flat :loanTerms :loanPrincipalAmount :: INT AS principal_amount,
        TO_TIMESTAMP(
            decoded_flat :loanTerms :loanStartTime
        ) AS loan_start_time,
        decoded_flat :loanTerms :maximumRepaymentAmount :: INT AS total_debt_amount,
        (
            (
                decoded_flat :loanTerms :maximumRepaymentAmount - decoded_flat :loanTerms :loanPrincipalAmount
            ) / decoded_flat :loanTerms :loanPrincipalAmount
        ) * 100 AS interest_rate_percentage,
        decoded_flat :loanTerms :nftCollateralContract :: STRING AS nft_address,
        decoded_flat :loanTerms :nftCollateralId :: STRING AS tokenid,
        decoded_flat :loanTerms :nftCollateralWrapper :: STRING AS nft_collateral_wrapper,
        _log_id,
        _inserted_timestamp
    FROM
        raw_logs
    WHERE
        event_name IN ('LoanStarted')
),
renegotiated AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        contract_address,
        decoded_flat,
        decoded_flat :borrower :: STRING AS borrower_address,
        decoded_flat :lender :: STRING AS lender_address,
        decoded_flat :loanId AS loanId,
        decoded_flat :newLoanDuration AS new_loan_duration,
        decoded_flat :newMaximumRepaymentAmount :: INT AS new_total_debt_amount,
        decoded_flat :renegotiationAdminFee :: INT AS renegotiationAdminFee,
        decoded_flat :renegotiationFee :: INT AS renegotiationFee,
        _log_id,
        _inserted_timestamp
    FROM
        raw_logs
    WHERE
        event_name IN ('LoanRenegotiated')
),
base_raw AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        contract_address,
        decoded_flat,
        borrower_address,
        lender_address,
        loanid,
        0 AS platform_fee_unadj,
        loan_start_time,
        loan_duration,
        DATEADD(
            seconds,
            loan_duration,
            loan_start_time
        ) AS loan_end_time,
        loan_denomination,
        principal_amount,
        total_debt_amount,
        interest_rate_percentage,
        nft_address,
        tokenid,
        nft_collateral_wrapper,
        'new_loan' AS event_type,
        _log_id,
        _inserted_timestamp
    FROM
        loan_started
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        contract_address,
        decoded_flat,
        borrower_address,
        lender_address,
        loanId,
        renegotiationAdminFee AS platform_fee_unadj,
        block_timestamp AS loan_start_time,
        new_loan_duration AS loan_duration,
        DATEADD(
            seconds,
            loan_duration,
            loan_start_time
        ) AS loan_end_time,
        NULL AS loan_denomination,
        NULL AS principal_amount,
        new_total_debt_amount AS total_debt_amount,
        NULL AS interest_rate_percentage,
        NULL AS nft_address,
        NULL AS tokenid,
        NULL AS nft_collateral_wrapper,
        'refinance' AS event_type,
        _log_id,
        _inserted_timestamp
    FROM
        renegotiated
),
base_fill AS (
    SELECT
        *,
        IFF(
            event_type = 'new_loan',
            principal_amount,
            LAG(principal_amount) ignore nulls over (
                PARTITION BY loanid,
                borrower_address
                ORDER BY
                    block_timestamp ASC
            )
        ) AS principal_amount_fill,
        IFF(
            event_type = 'new_loan',
            loan_denomination,
            LAG(loan_denomination) ignore nulls over (
                PARTITION BY loanid,
                borrower_address
                ORDER BY
                    block_timestamp ASC
            )
        ) AS loan_denomination_fill,
        IFF(
            event_type = 'new_loan',
            nft_address,
            LAG(nft_address) ignore nulls over (
                PARTITION BY loanid,
                borrower_address
                ORDER BY
                    block_timestamp ASC
            )
        ) AS nft_address_fill,
        IFF(event_type = 'new_loan', tokenid, LAG(tokenid) ignore nulls over (PARTITION BY loanid, borrower_address
    ORDER BY
        block_timestamp ASC)) AS tokenid_fill,
        IFF(
            event_type = 'new_loan',
            nft_collateral_wrapper,
            LAG(nft_collateral_wrapper) ignore nulls over (
                PARTITION BY loanid,
                borrower_address
                ORDER BY
                    block_timestamp ASC
            )
        ) AS nft_collateral_wrapper_fill,
        IFF(
            event_type = 'new_loan',
            interest_rate_percentage,
            ((total_debt_amount - principal_amount_fill) / principal_amount_fill) * 100
        ) AS interest_rate_percentage_fill
    FROM
        base_raw
),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        'nftfi' AS platform_name,
        contract_address AS platform_address,
        'nftfi v2' AS platform_exchange_version,
        contract_address,
        decoded_flat,
        borrower_address,
        lender_address,
        loanid,
        platform_fee_unadj,
        loan_start_time AS loan_start_timestamp,
        loan_duration AS loan_tenure,
        loan_end_time AS loan_due_timestamp,
        loan_denomination_fill AS loan_token_address,
        principal_amount_fill AS principal_amount_unadj,
        total_debt_amount AS total_debt_unadj,
        interest_rate_percentage_fill AS interest_rate_percentage,
        interest_rate_percentage_fill / pow(
            10,
            2
        ) AS interest_rate,
        interest_rate_percentage_fill * pow(
            10,
            2
        ) AS interest_rate_bps,
        nft_address_fill AS nft_address,
        tokenid_fill AS tokenid,
        nft_collateral_wrapper_fill AS nft_collateral_wrapper,
        event_type,
        'fixed' AS loan_term_type,
        _log_id,
        _inserted_timestamp
    FROM
        base_fill
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    platform_name,
    platform_address,
    platform_exchange_version,
    contract_address,
    decoded_flat,
    borrower_address,
    lender_address,
    loanid,
    platform_fee_unadj,
    loan_start_timestamp,
    loan_tenure,
    loan_due_timestamp,
    loan_token_address,
    principal_amount_unadj,
    total_debt_unadj,
    interest_rate_percentage,
    interest_rate,
    interest_rate_bps,
    nft_address,
    tokenid,
    nft_collateral_wrapper,
    event_type,
    loan_term_type,
    LAG(block_timestamp) over (
        PARTITION BY loanId
        ORDER BY
            block_timestamp ASC
    ) AS prev_block_timestamp,
    _log_id,
    _inserted_timestamp,
    CONCAT(
        loanid,
        '-',
        _log_id
    ) AS nft_lending_id,
    {{ dbt_utils.generate_surrogate_key(
        ['loanid', 'borrower_address', 'lender_address','nft_address','tokenId','platform_exchange_version']
    ) }} AS unique_loan_id
FROM
    FINAL
