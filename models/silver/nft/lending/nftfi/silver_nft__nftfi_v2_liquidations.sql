{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

SELECT
    tx_hash,
    contract_address,
    decoded_flat,
    decoded_flat :borrower :: STRING AS borrower_address,
    decoded_flat :lender :: STRING AS lender_address,
    decoded_flat :loanId AS loanid,
    decoded_flat :loanLiquidationDate AS loanLiquidationDate,
    decoded_flat :loanMaturityDate AS loanMaturityDate,
    decoded_flat :loanPrincipalAmount :: INT AS loanPrincipalAmount,
    decoded_flat :nftCollateralContract :: STRING AS nftCollateralContract,
    decoded_flat :nftCollateralId AS nftCollateralId
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
    AND event_name IN ('LoanLiquidated')
