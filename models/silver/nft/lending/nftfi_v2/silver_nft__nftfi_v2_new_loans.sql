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
            'LoanStarted'
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
        decoded_flat :loanId :: STRING AS loanid,
        decoded_flat :loanTerms :borrower :: STRING AS loanterms_borrower_address,
        decoded_flat :loanTerms :loanAdminFeeInBasisPoints AS admin_fee_bps,
        decoded_flat :loanTerms :loanDuration AS loan_duration,
        decoded_flat :loanTerms :loanERC20Denomination :: STRING AS loan_denomination,
        decoded_flat :loanTerms :loanInterestRateForDurationInBasisPoints AS loanInterestRateForDurationInBasisPoints,
        decoded_flat :loanTerms :loanPrincipalAmount :: INT AS principal_amount,
        TO_TIMESTAMP(
            decoded_flat :loanTerms :loanStartTime
        ) AS loan_start_time,
        decoded_flat :loanTerms :maximumRepaymentAmount :: INT AS debt_amount,
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
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    decoded_flat,
    contract_address,
    borrower_address,
    lender_address,
    referral_fee_bps,
    revenue_share_bps,
    revenue_share_address,
    loanid,
    loanterms_borrower_address,
    admin_fee_bps,
    loan_duration,
    loan_denomination,
    loanInterestRateForDurationInBasisPoints,
    principal_amount,
    loan_start_time,
    debt_amount,
    interest_rate_percentage,
    nft_address,
    tokenid,
    nft_collateral_wrapper,
    _log_id,
    _inserted_timestamp
FROM
    loan_started
