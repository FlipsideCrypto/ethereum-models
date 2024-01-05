{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH raw_logs AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        contract_address,
        'nftfi' AS platform_name,
        contract_address AS platform_address,
        'nftfi v1' AS platform_exchange_version,
        decoded_flat,
        decoded_flat :borrower :: STRING AS borrower_address,
        decoded_flat :lender :: STRING AS lender_address,
        decoded_flat :loanId AS loanId,
        TO_TIMESTAMP(
            decoded_flat :loanLiquidationDate
        ) AS loan_liquidation_date,
        TO_TIMESTAMP(
            decoded_flat :loanMaturityDate
        ) AS loan_maturity_date,
        decoded_flat :loanPrincipalAmount :: INT AS principal_amount_unadj,
        decoded_flat :nftCollateralContract :: STRING AS nft_address,
        decoded_flat :nftCollateralId AS tokenId,
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
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_timestamp >= '2020-05-01'
        AND contract_address = '0x88341d1a8f672d2780c8dc725902aae72f143b0c'
        AND event_name = 'LoanLiquidated'

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
    l.block_number,
    l.block_timestamp,
    l.tx_hash,
    l.event_index,
    l.event_name,
    l.platform_name,
    l.platform_address,
    l.platform_exchange_version,
    l.contract_address,
    l.decoded_flat,
    l.borrower_address,
    l.lender_address,
    l.loanId,
    b.loan_start_timestamp,
    b.loan_tenure,
    b.loan_due_timestamp,
    b.interest_rate_percentage,
    b.interest_rate,
    b.interest_rate_bps,
    b.lender_address AS previous_lender_address,
    l.loan_liquidation_date,
    l.loan_maturity_date,
    l.principal_amount_unadj,
    b.loan_token_address,
    l.nft_address,
    l.tokenId,
    l._log_id,
    l._inserted_timestamp,
    l.nft_lending_id,
    l.unique_loan_id
FROM
    raw_logs l
    INNER JOIN {{ ref('silver_nft__nftfi_v1_loans_taken') }}
    b
    ON l.loanId = b.loanId
    AND (
        (
            b.prev_block_timestamp IS NULL
            AND l.block_timestamp > b.block_timestamp
        )
        OR (
            l.block_timestamp > b.block_timestamp
            AND b.prev_block_timestamp > l.block_timestamp
        )
    )
