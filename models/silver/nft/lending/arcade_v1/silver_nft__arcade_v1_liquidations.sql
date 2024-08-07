{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        contract_address,
        decoded_flat,
        decoded_flat :loanId :: INT AS loanid,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS rn,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_timestamp :: DATE >= '2021-08-30'
        AND contract_address IN (
            '0x59e57f9a313a2eb1c7357ecc331ddca14209f403',
            -- v1 . called as pawnfi , only has 50 events. need to join both events to get borrower and lender. join on txhash and loanid
            '0x7691ee8febd406968d46f9de96cb8cc18fc8b325',
            -- v1.2
            '0x606e4a441290314aeaf494194467fd2bb844064a' -- v1.1? flash rollover , has the same collateral token as v1 . in this rollover, old loan is repaid, new loan is created
        )
        AND event_name IN (
            'LoanClaimed'
        )
)
SELECT
    b.block_timestamp,
    b.tx_hash,
    b.event_index,
    b.event_name,
    contract_address,
    loanid,
    version_num,
    borrower_address,
    lender_address,
    collateral_token_address,
    collateral_tokenid,
    duration_seconds,
    interest_amount,
    loan_currency_address,
    principal_amount,
    origination_fee_bps,
    origination_fee,
    net_principal_amount
FROM
    base b
    INNER JOIN {{ ref('silver_nft__arcade_v1_loans') }}
    l USING (
        contract_address,
        loanid
    )
