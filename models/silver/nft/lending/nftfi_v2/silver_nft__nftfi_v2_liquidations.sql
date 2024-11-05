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
        'nftfi v2' AS platform_exchange_version,
        decoded_log AS decoded_flat,
        decoded_flat :borrower :: STRING AS borrower_address,
        decoded_flat :lender :: STRING AS lender_address,
        decoded_flat :loanId AS loanid,
        TO_TIMESTAMP(
            decoded_flat :loanLiquidationDate
        ) AS loan_liquidation_date,
        TO_TIMESTAMP(
            decoded_flat :loanMaturityDate
        ) AS loan_maturity_date,
        decoded_flat :loanPrincipalAmount :: INT AS principal_unadj,
        decoded_flat :nftCollateralContract :: STRING AS nft_address,
        decoded_flat :nftCollateralId :: STRING AS tokenId,
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
        block_timestamp >= '2022-03-01'
        AND contract_address IN (
            LOWER('0xd0a40eB7FD94eE97102BA8e9342243A2b2E22207'),
            LOWER('0x8252Df1d8b29057d1Afe3062bf5a64D503152BC8'),
            LOWER('0xf896527c49b44aAb3Cf22aE356Fa3AF8E331F280'),
            LOWER('0xD0C6e59B50C32530C627107F50Acc71958C4341F'),
            LOWER('0xe52cec0e90115abeb3304baa36bc2655731f7934')
        )
        AND event_name IN ('LoanLiquidated')

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
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
        loan_due_timestamp,
        loan_token_address
    FROM
        {{ ref('silver_nft__nftfi_v2_loans') }}
        qualify ROW_NUMBER() over (
            PARTITION BY loanid,
            nft_address,
            tokenid,
            borrower_address
            ORDER BY
                block_timestamp DESC
        ) = 1
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
    b.loan_token_address,
    b.interest_rate_percentage,
    b.interest_rate,
    b.interest_rate_bps,
    b.annual_percentage_rate,
    b.lender_address AS previous_lender_address,
    l.loan_liquidation_date,
    l.loan_maturity_date,
    l.principal_unadj,
    l.nft_address,
    l.tokenId,
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
