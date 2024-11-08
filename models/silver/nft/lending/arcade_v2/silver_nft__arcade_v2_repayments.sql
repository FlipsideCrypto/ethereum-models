{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['stale']
) }}

WITH base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        contract_address,
        decoded_log AS decoded_flat,
        decoded_flat :loanId :: STRING AS loanid,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS rn,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2022-06-20'
        AND contract_address IN (
            '0x81b2f8fc75bab64a6b144aa6d2faa127b4fa7fd9'
        )
        AND event_name IN (
            'LoanRepaid'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    b.block_number,
    b.block_timestamp,
    b.tx_hash,
    b.event_index,
    'repay' AS event_type,
    b.contract_address,
    b.event_name,
    platform_name,
    platform_address,
    platform_exchange_version,
    loanid,
    nft_address,
    tokenid,
    lender_address,
    borrower_address,
    principal_unadj,
    debt_unadj,
    platform_fee_unadj,
    loan_token_address,
    interest_rate_percentage,
    annual_percentage_rate,
    loan_term_type,
    loan_start_timestamp,
    loan_due_timestamp,
    deadline_loan_due_timestamp,
    block_timestamp AS loan_paid_timestamp,
    b._log_id,
    b._inserted_timestamp,
    CONCAT(
        loanid,
        '-',
        _log_id
    ) AS nft_lending_id,
    {{ dbt_utils.generate_surrogate_key(
        ['loanid', 'borrower_address', 'lender_address', 'nft_address','tokenId','platform_exchange_version']
    ) }} AS unique_loan_id
FROM
    base b
    INNER JOIN {{ ref('silver_nft__arcade_v2_loans') }}
    l USING (
        loanid
    )
