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
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2021-08-30'
        AND contract_address IN (
            '0x59e57f9a313a2eb1c7357ecc331ddca14209f403',
            -- v1 . called as pawnfi
            '0x7691ee8febd406968d46f9de96cb8cc18fc8b325',
            -- v1.2
            '0x606e4a441290314aeaf494194467fd2bb844064a' -- v1.1 flash rollover
        )
        AND event_name IN (
            'LoanCreated',
            'LoanStarted'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
loan_created AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        contract_address,
        decoded_flat :loanId :: STRING AS loanid,
        decoded_flat :terms :collateralTokenId :: STRING AS collateral_tokenid,
        decoded_flat :terms :durationSecs :: INT AS duration_seconds,
        decoded_flat :terms :interest :: INT AS interest_amount,
        decoded_flat :terms :payableCurrency :: STRING AS loan_currency_address,
        decoded_flat :terms :principal :: INT AS principal_amount,
        _log_id,
        _inserted_timestamp
    FROM
        base
    WHERE
        event_name = 'LoanCreated'
),
loan_started AS (
    SELECT
        tx_hash,
        decoded_flat :borrower :: STRING AS borrower_raw,
        decoded_flat :lender :: STRING AS lender_raw,
        decoded_flat :loanId :: STRING AS loanId
    FROM
        base
    WHERE
        event_name = 'LoanStarted'
),
loan_details AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        contract_address,
        CASE
            WHEN contract_address = '0x59e57f9a313a2eb1c7357ecc331ddca14209f403' THEN 'v1'
            WHEN contract_address = '0x7691ee8febd406968d46f9de96cb8cc18fc8b325' THEN 'v1.2'
            WHEN contract_address = '0x606e4a441290314aeaf494194467fd2bb844064a' THEN 'v1.1'
            ELSE NULL
        END AS version_num,
        borrower_raw,
        lender_raw,
        loanid,
        CASE
            WHEN contract_address IN (
                '0x59e57f9a313a2eb1c7357ecc331ddca14209f403',
                '0x606e4a441290314aeaf494194467fd2bb844064a'
            ) THEN '0x1f563cdd688ad47b75e474fde74e87c643d129b7'
            WHEN contract_address = '0x7691ee8febd406968d46f9de96cb8cc18fc8b325' THEN '0x5cb803c31e8f4f895a3ab19d8218646dc63e9dc2'
            ELSE NULL
        END AS collateral_token_address,
        collateral_tokenid,
        duration_seconds,
        interest_amount,
        loan_currency_address,
        principal_amount,
        _log_id,
        _inserted_timestamp
    FROM
        loan_created
        INNER JOIN loan_started USING (tx_hash)
),
note_ownership_raw AS (
    SELECT
        tx_hash,
        tokenid AS loanid,
        contract_address,
        to_address
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        block_timestamp :: DATE >= '2021-08-30'
        AND contract_address IN (
            '0x6bd1476dd1d57f08670af6720ca2edf37b10746e',
            -- lender for v1.1
            '0xe00b37ad3a165a66c20ca3e0170e4749c20ef58c',
            -- borrower for v1.1
            '0xc3231258d6ed397dce7a52a27f816c8f41d22151',
            -- borrower for v1.2
            '0xe1ef2656d965ac9e3fe151312f19f3d4c5f0efa3' -- lender for v1.2
        ) qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            loanid,
            contract_address
            ORDER BY
                event_index DESC
        ) = 1
),
note_ownership AS (
    -- for v1.1 and v1.2 only
    SELECT
        tx_hash,
        loanid,
        MAX(
            IFF(
                contract_address IN (
                    '0x6bd1476dd1d57f08670af6720ca2edf37b10746e',
                    '0xe1ef2656d965ac9e3fe151312f19f3d4c5f0efa3'
                ),
                to_address,
                NULL
            )
        ) AS lender_address_raw,
        MAX(
            IFF(
                contract_address IN (
                    '0xe00b37ad3a165a66c20ca3e0170e4749c20ef58c',
                    '0xc3231258d6ed397dce7a52a27f816c8f41d22151'
                ),
                to_address,
                NULL
            )
        ) AS borrower_address_raw
    FROM
        note_ownership_raw
    GROUP BY
        ALL
),
origination_fee AS (
    SELECT
        tx_hash,
        trace_index,
        decoded_data :function_name :: STRING AS function_name,
        decoded_data :decoded_output_data :output_1 :: INT AS origination_fee_bps
    FROM
        {{ ref('core__ez_decoded_traces') }}
    WHERE
        block_timestamp :: DATE >= '2021-07-16'
        AND from_address IN (
            '0x606e4a441290314aeaf494194467fd2bb844064a',
            --LoanCore 1.5
            '0x59e57f9a313a2eb1c7357ecc331ddca14209f403',
            --arcade loancore v1
            '0x27ed938ff4d532332c2701866d7869edcb39d7e4',
            -- v1.2.1 but unverified
            '0x7691ee8febd406968d46f9de96cb8cc18fc8b325' --loancore v1.2
        )
        AND to_address IN (
            '0xfc2b8d5c60c8e8bbf8d6dc685f03193472e39587',
            -- FeeController - v1, 1.1
            '0x4cccc5c5ef1d8c4a6ad6765a36651ef523e42e75' -- fee controller v1.2
        )
        AND function_name = 'getOriginationFee'
        AND trace_succeeded
        AND tx_succeeded qualify ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) = 1
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    'arcade' AS platform_name,
    contract_address,
    contract_address AS platform_address,
    'arcade v1' AS platform_exchange_version,
    version_num,
    borrower_raw,
    lender_raw,
    borrower_address_raw,
    lender_address_raw,
    IFF(
        borrower_address_raw IS NULL,
        borrower_raw,
        borrower_address_raw
    ) AS borrower_address,
    IFF(
        lender_address_raw IS NULL,
        lender_raw,
        lender_address_raw
    ) AS lender_address,
    loanid :: STRING AS loanid,
    collateral_token_address AS nft_address,
    collateral_tokenid AS tokenid,
    principal_amount AS principal_unadj,
    interest_amount,
    principal_unadj + interest_amount AS debt_unadj,
    duration_seconds,
    loan_currency_address AS loan_token_address,
    interest_amount / principal_unadj * 100 AS interest_rate_percentage,
    interest_rate_percentage / (
        duration_seconds / 86400
    ) * 365 AS annual_percentage_rate,
    origination_fee_bps,
    principal_amount * (origination_fee_bps / pow(10, 4)) AS origination_fee,
    origination_fee AS platform_fee_unadj,
    principal_amount - origination_fee AS net_principal_amount,
    'new_loan' AS event_type,
    'fixed' AS loan_term_type,
    block_timestamp AS loan_start_timestamp,
    DATEADD(
        seconds,
        duration_seconds,
        loan_start_timestamp
    ) AS loan_due_timestamp,
    duration_seconds AS loan_tenure,
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
    loan_details
    LEFT JOIN origination_fee USING (tx_hash)
    LEFT JOIN note_ownership USING (
        tx_hash,
        loanid
    )
