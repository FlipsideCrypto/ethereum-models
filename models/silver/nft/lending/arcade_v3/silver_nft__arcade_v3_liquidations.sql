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
        block_timestamp :: DATE >= '2023-08-29'
        AND contract_address IN (
            LOWER('0x89bc08BA00f135d608bc335f6B33D7a9ABCC98aF')
        )
        AND event_name IN (
            'LoanClaimed'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    b.block_number,
    b.block_timestamp,
    b.tx_hash,
    b.event_index,
    b.event_name,
    platform_name,
    platform_address,
    platform_exchange_version,
    b.contract_address,
    b.decoded_flat,
    loanid,
    lender_address,
    borrower_address,
    nft_address,
    tokenid,
    principal_unadj,
    loan_token_address,
    interest_rate_percentage,
    annual_percentage_rate,
    loan_term_type,
    loan_start_timestamp,
    deadline_loan_due_timestamp,
    loan_tenure,
    loan_due_timestamp,
    b._log_id,
    b._inserted_timestamp,
    CONCAT(
        loanid,
        '-',
        b._log_id
    ) AS nft_lending_id,
    {{ dbt_utils.generate_surrogate_key(
        ['loanid', 'borrower_address', 'lender_address', 'nft_address','tokenId','platform_exchange_version']
    ) }} AS unique_loan_id
FROM
    base b
    INNER JOIN {{ ref('silver_nft__arcade_v3_loans') }}
    l USING (
        loanid
    )
