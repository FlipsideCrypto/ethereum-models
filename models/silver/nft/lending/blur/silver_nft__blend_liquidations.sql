{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH all_loans AS (

    SELECT
        block_timestamp,
        tx_hash,
        nft_address,
        lienid,
        borrower_address,
        tokenid,
        category {# prev_lender_address,
        prev_loan_amount,
        prev_interest_rate #}
    FROM
        {{ ref('silver_nft__blend_loans_taken') }}
),
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_name,
    contract_address,
    decoded_flat,
    decoded_flat :collection :: STRING AS collection_address,
    decoded_flat :lienId AS lienId
FROM
    {{ ref('silver__decoded_logs') }}
WHERE
    block_timestamp :: DATE >= '2023-05-01'
    AND contract_address = '0x29469395eaf6f95920e59f858042f0e28d98a20b'
    AND event_name = 'Seize'
LIMIT
    10 TO_TIMESTAMP(
        decoded_flat :loanLiquidationDate
    ) AS loan_liquidation_date,
    TO_TIMESTAMP(
        decoded_flat :loanMaturityDate
    ) AS loan_maturity_date,
    decoded_flat :loanPrincipalAmount :: INT AS loan_principal_amount,
    decoded_flat :nftCollateralContract :: STRING AS collateral_nft_address,
    decoded_flat :nftCollateralId AS collateral_token_id,
