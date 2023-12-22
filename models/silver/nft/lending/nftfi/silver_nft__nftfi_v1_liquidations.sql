{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

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
    TO_TIMESTAMP(
        decoded_flat :loanLiquidationDate
    ) AS loan_liquidation_date,
    TO_TIMESTAMP(
        decoded_flat :loanMaturityDate
    ) AS loan_maturity_date,
    decoded_flat :loanPrincipalAmount :: INT AS loan_principal_amount,
    decoded_flat :nftCollateralContract :: STRING AS collateral_nft_address,
    decoded_flat :nftCollateralId AS collateral_token_id,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__decoded_logs') }}
WHERE
    block_timestamp >= '2020-05-01'
    AND contract_address = '0x88341d1a8f672d2780c8dc725902aae72f143b0c'
    AND event_name = 'LoanLiquidated'
