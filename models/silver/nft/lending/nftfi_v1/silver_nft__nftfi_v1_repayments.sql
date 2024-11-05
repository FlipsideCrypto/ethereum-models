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
        decoded_log AS decoded_flat,
        decoded_flat :adminFee :: INT AS platform_fee_unadj,
        decoded_flat :amountPaidToLender :: INT AS amount_paid_to_lender,
        amount_paid_to_lender + platform_fee_unadj AS debt_unadj,
        decoded_flat :borrower :: STRING AS borrower_address,
        decoded_flat :lender :: STRING AS lender_address,
        decoded_flat: loanERC20Denomination :: STRING AS loan_token_address,
        decoded_flat: loanId :: STRING AS loanId,
        decoded_flat: loanPrincipalAmount :: INT AS principal_unadj,
        decoded_flat :nftCollateralContract :: STRING AS nft_address,
        decoded_flat: nftCollateralId :: STRING AS tokenId,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp,
        CONCAT(
            loanid,
            '-',
            _log_id
        ) AS nft_lending_id,
        {{ dbt_utils.generate_surrogate_key(
            ['loanid', 'borrower_address', 'nft_address','tokenId','platform_exchange_version']
        ) }} AS unique_loan_id
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        block_timestamp >= '2020-05-01'
        AND contract_address = '0x88341d1a8f672d2780c8dc725902aae72f143b0c'
        AND event_name = 'LoanRepaid'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '24 hours'
    FROM
        {{ this }}
)
{% endif %}
),
loan_details AS (
    SELECT
        loanid,
        nft_address,
        tokenid,
        lender_address,
        borrower_address,
        interest_rate_percentage,
        interest_rate,
        interest_rate_bps,
        annual_percentage_rate,
        loan_start_timestamp,
        loan_tenure,
        loan_due_timestamp
    FROM
        {{ ref('silver_nft__nftfi_v1_loans') }}
        qualify ROW_NUMBER() over (
            PARTITION BY loanid,
            nft_address,
            tokenid,
            borrower_address
            ORDER BY
                block_timestamp DESC
        ) = 1
),
FINAL AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.event_name,
        l.event_index,
        l.contract_address,
        l.platform_name,
        l.platform_address,
        l.platform_exchange_version,
        l.decoded_flat,
        l.loanId,
        l.nft_address,
        l.tokenId,
        l.borrower_address,
        l.lender_address,
        b.lender_address AS prev_lender_address,
        l.loan_token_address,
        l.principal_unadj,
        l.debt_unadj,
        l.platform_fee_unadj,
        l.amount_paid_to_lender,
        b.interest_rate_percentage,
        b.interest_rate,
        b.interest_rate_bps,
        b.annual_percentage_rate,
        b.loan_start_timestamp,
        b.loan_tenure,
        b.loan_due_timestamp,
        l.block_timestamp AS loan_paid_timestamp,
        l._log_id,
        l._inserted_timestamp,
        l.nft_lending_id,
        l.unique_loan_id
    FROM
        raw_logs l
        INNER JOIN loan_details b USING (
            loanid,
            nft_address,
            tokenid,
            borrower_address
        )
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_name,
    event_index,
    'repay' AS event_type,
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
    prev_lender_address,
    loan_token_address,
    principal_unadj,
    debt_unadj,
    platform_fee_unadj,
    amount_paid_to_lender,
    interest_rate_percentage,
    interest_rate,
    interest_rate_bps,
    annual_percentage_rate,
    'fixed' AS loan_term_type,
    loan_start_timestamp,
    loan_tenure,
    loan_due_timestamp,
    loan_paid_timestamp,
    _log_id,
    _inserted_timestamp,
    nft_lending_id,
    unique_loan_id
FROM
    FINAL
