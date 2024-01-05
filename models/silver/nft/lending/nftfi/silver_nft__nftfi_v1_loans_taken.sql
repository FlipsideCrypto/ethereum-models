{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['stale']
) }}

WITH raw_logs AS (

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
        decoded_flat :interestIsProRated AS interest_is_prorated,
        decoded_flat :loanDuration AS loan_tenure,
        decoded_flat :loanERC20Denomination :: STRING AS loan_token_address,
        decoded_flat :loanId :: STRING AS loanId,
        decoded_flat :loanInterestRateForDurationInBasisPoints AS loanInterestRateForDurationInBasisPoints,
        decoded_flat :loanPrincipalAmount :: INT AS principal_amount_unadj,
        TO_TIMESTAMP(
            decoded_flat :loanStartTime
        ) AS loan_start_timestamp,
        DATEADD(
            seconds,
            loan_tenure,
            loan_start_timestamp
        ) AS loan_due_timestamp,
        decoded_flat :maximumRepaymentAmount :: INT AS total_debt_unadj,
        100 * div0(
            (
                decoded_flat :maximumRepaymentAmount - decoded_flat :loanPrincipalAmount
            ),(
                decoded_flat :loanPrincipalAmount
            )
        ) AS interest_rate_percentage,
        interest_rate_percentage / pow(
            10,
            2
        ) AS interest_rate,
        interest_rate_percentage * pow(
            10,
            2
        ) AS interest_rate_bps,
        decoded_flat :nftCollateralContract :: STRING AS nft_address,
        decoded_flat :nftCollateralId :: STRING AS tokenid,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_timestamp >= '2020-05-01'
        AND contract_address = '0x88341d1a8f672d2780c8dc725902aae72f143b0c'
        AND event_name = 'LoanStarted'

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
    event_index,
    event_name,
    'nftfi' AS platform_name,
    contract_address AS platform_address,
    'nftfi v1' AS platform_exchange_version,
    contract_address,
    decoded_flat,
    borrower_address,
    lender_address,
    loanid,
    0 AS platform_fee_unadj,
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
    'new_loan' AS event_type,
    'fixed' AS loan_term_type,
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
        ['loanid', 'borrower_address', 'lender_address', 'nft_address','tokenId','platform_exchange_version']
    ) }} AS unique_loan_id
FROM
    raw_logs
