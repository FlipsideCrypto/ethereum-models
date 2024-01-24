{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH raw_logs AS (

    SELECT
        *
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
        AND event_name IN (
            'LoanRenegotiated'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '24 hours'
    FROM
        {{ this }}
)
{% endif %}
),
renegotiated AS (
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
        decoded_flat :loanId :: STRING AS loanId,
        decoded_flat :newLoanDuration AS new_loan_duration,
        decoded_flat :newMaximumRepaymentAmount :: INT AS new_debt_amount,
        decoded_flat :renegotiationAdminFee :: INT AS renegotiationAdminFee,
        decoded_flat :renegotiationFee :: INT AS renegotiationFee,
        _log_id,
        _inserted_timestamp
    FROM
        raw_logs
),
loan_details AS (
    SELECT
        *,
        LEAD(block_timestamp) over (
            PARTITION BY loanid
            ORDER BY
                block_timestamp ASC
        ) AS next_block_timestamp
    FROM
        {{ ref('silver_nft__nftfi_v2_new_loans') }}
),
renegotiated_details AS (
    SELECT
        r.block_number,
        r.block_timestamp,
        r.tx_hash,
        r.event_index,
        r.event_name,
        r.contract_address,
        r.decoded_flat,
        r.borrower_address,
        r.lender_address,
        r.loanId,
        r.new_loan_duration,
        r.new_debt_amount,
        r.renegotiationAdminFee,
        r.renegotiationFee,
        b.loan_denomination,
        b.principal_amount,
        b.interest_rate_percentage,
        b.nft_address,
        b.tokenid,
        b.nft_collateral_wrapper,
        r._log_id,
        r._inserted_timestamp
    FROM
        renegotiated r
        INNER JOIN loan_details b
        ON r.loanid = b.loanid
        AND r.borrower_address = b.borrower_address
        AND r.block_timestamp > b.block_timestamp
        AND (
            r.block_timestamp < b.next_block_timestamp
            OR b.next_block_timestamp IS NULL
        )
),
base_raw AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        contract_address,
        decoded_flat,
        borrower_address,
        lender_address,
        loanid,
        0 AS platform_fee_unadj,
        loan_start_time,
        loan_duration,
        DATEADD(
            seconds,
            loan_duration,
            loan_start_time
        ) AS loan_end_time,
        loan_denomination,
        principal_amount,
        debt_amount,
        interest_rate_percentage,
        nft_address,
        tokenid,
        nft_collateral_wrapper,
        'new_loan' AS event_type,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_nft__nftfi_v2_new_loans') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '24 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    decoded_flat,
    borrower_address,
    lender_address,
    loanId,
    renegotiationAdminFee AS platform_fee_unadj,
    block_timestamp AS loan_start_time,
    new_loan_duration AS loan_duration,
    DATEADD(
        seconds,
        loan_duration,
        loan_start_time
    ) AS loan_end_time,
    loan_denomination,
    principal_amount,
    new_debt_amount AS debt_amount,
    interest_rate_percentage,
    nft_address,
    tokenid,
    nft_collateral_wrapper,
    'refinance' AS event_type,
    _log_id,
    _inserted_timestamp
FROM
    renegotiated_details
),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        'nftfi' AS platform_name,
        contract_address AS platform_address,
        'nftfi v2' AS platform_exchange_version,
        contract_address,
        decoded_flat,
        borrower_address,
        lender_address,
        loanid,
        platform_fee_unadj,
        loan_start_time AS loan_start_timestamp,
        loan_duration AS loan_tenure,
        loan_end_time AS loan_due_timestamp,
        loan_denomination AS loan_token_address,
        principal_amount AS principal_unadj,
        debt_amount AS debt_unadj,
        interest_rate_percentage,
        interest_rate_percentage / pow(
            10,
            2
        ) AS interest_rate,
        interest_rate_percentage * pow(
            10,
            2
        ) AS interest_rate_bps,
        (
            (
                (
                    (
                        debt_unadj - principal_unadj
                    ) / loan_tenure
                ) * 31536000
            ) / principal_unadj
        ) * 100 AS annual_percentage_rate,
        nft_address,
        tokenid,
        nft_collateral_wrapper,
        event_type,
        'fixed' AS loan_term_type,
        _log_id,
        _inserted_timestamp
    FROM
        base_raw
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    platform_name,
    platform_address,
    platform_exchange_version,
    contract_address,
    decoded_flat,
    borrower_address,
    lender_address,
    loanid,
    platform_fee_unadj,
    loan_start_timestamp,
    loan_tenure,
    loan_due_timestamp,
    loan_token_address,
    principal_unadj,
    debt_unadj,
    interest_rate_percentage,
    interest_rate,
    interest_rate_bps,
    annual_percentage_rate,
    nft_address,
    tokenid,
    nft_collateral_wrapper,
    event_type,
    loan_term_type,
    _log_id,
    _inserted_timestamp,
    CONCAT(
        loanid,
        '-',
        _log_id
    ) AS nft_lending_id,
    {{ dbt_utils.generate_surrogate_key(
        ['loanid', 'borrower_address', 'nft_address','tokenId','platform_exchange_version']
    ) }} AS unique_loan_id
FROM
    FINAL
