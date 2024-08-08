{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH base AS (

    SELECT
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        contract_address,
        decoded_flat,
        decoded_flat :loanId :: STRING AS loanid,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS rn
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_timestamp :: DATE >= '2022-06-20'
        AND contract_address IN (
            '0x81b2f8fc75bab64a6b144aa6d2faa127b4fa7fd9'
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
    b.contract_address,
    borrower,
    lender,
    collateral_token_address,
    collateral_tokenid,
    deadline,
    DURATION,
    interest_rate,
    num_installments,
    loan_currency,
    principal,
    loanid,
    interest_raw,
    fee_function_name,
    origination_fee_bps
FROM
    base b
    INNER JOIN {{ ref('silver_nft__arcade_v2_loans') }}
    l USING (
        loanid
    )
