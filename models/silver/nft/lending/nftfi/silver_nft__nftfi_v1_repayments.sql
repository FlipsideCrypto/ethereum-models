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
        event_name,
        event_index,
        contract_address,
        'nftfi' AS platform_name,
        contract_address AS platform_address,
        'nftfi v1' AS platform_exchange_version,
        decoded_flat,
        decoded_flat :adminFee :: INT AS platform_fee_unadj,
        decoded_flat :amountPaidToLender :: INT AS amount_paid_to_lender,
        amount_paid_to_lender + platform_fee_unadj AS total_debt_unadj,
        decoded_flat :borrower :: STRING AS borrower_address,
        decoded_flat :lender :: STRING AS lender_address,
        decoded_flat: loanERC20Denomination :: STRING AS loan_token_address,
        decoded_flat: loanId :: STRING AS loanId,
        decoded_flat: loanPrincipalAmount :: INT AS principal_amount_unadj,
        decoded_flat :nftCollateralContract :: STRING AS nft_address,
        decoded_flat: nftCollateralId :: STRING AS tokenId,
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
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_timestamp >= '2020-05-01'
        AND contract_address = '0x88341d1a8f672d2780c8dc725902aae72f143b0c'
        AND event_name = 'LoanRepaid'
),
loanid_details AS (
    SELECT
        unique_loan_id,
        loan_start_timestamp,
        loan_tenure,
        loan_due_timestamp,
        interest_rate_percentage,
        interest_rate,
        interest_rate_bps
    FROM
        {{ ref('silver_nft__nftfi_v1_loans_taken') }}
        qualify ROW_NUMBER() over (
            PARTITION BY unique_loan_id
            ORDER BY
                block_timestamp DESC
        ) = 1
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_name,
    event_index,
    contract_address,
    platform_name,
    platform_address,
    platform_exchange_version,
    decoded_flat,
    loanId,
    nft_address,
    tokenId,
    borrower_address,
    lender_address,
    loan_token_address,
    principal_amount_unadj,
    total_debt_unadj,
    platform_fee_unadj,
    amount_paid_to_lender,
    interest_rate_percentage,
    interest_rate,
    interest_rate_bps,
    loan_start_timestamp,
    loan_tenure,
    loan_due_timestamp,
    block_timestamp AS loan_paid_timestamp,
    _log_id,
    _inserted_timestamp,
    nft_lending_id,
    unique_loan_id
FROM
    raw_logs
    INNER JOIN loanid_details USING (unique_loan_id)
