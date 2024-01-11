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
        block_timestamp :: DATE >= '2023-05-01'
        AND contract_address = '0x29469395eaf6f95920e59f858042f0e28d98a20b'
        AND event_name IN (
            'LoanOfferTaken',
            'Refinance'
        )
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '24 hours'
    FROM
        {{ this }}
)
{% endif %}
),
loan_offer_taken_raw AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        decoded_flat,
        decoded_flat :auctionDuration AS auction_duration_blocks,
        decoded_flat :borrower :: STRING AS borrower_address,
        decoded_flat :lender :: STRING AS lender_address,
        decoded_flat :lienId :: STRING AS lienid,
        decoded_flat :collection :: STRING AS nft_address,
        decoded_flat :tokenId :: STRING AS tokenId,
        decoded_flat :loanAmount :: INT AS loan_amount,
        decoded_flat :rate :: INT AS interest_rate_bps,
        interest_rate_bps / pow(
            10,
            4
        ) AS interest_rate,
        decoded_flat :offerHash :: STRING AS offerhash,
        'new_loan' AS event_type,
        _log_id,
        _inserted_timestamp
    FROM
        raw_logs
    WHERE
        event_name = 'LoanOfferTaken'
),
refinance_raw AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        decoded_flat,
        decoded_flat :newAuctionDuration AS auction_duration_blocks,
        NULL AS borrower_address,
        decoded_flat :newLender :: STRING AS lender_address,
        decoded_flat :lienId :: STRING AS lienid,
        decoded_flat :collection :: STRING AS nft_address,
        NULL AS tokenId,
        decoded_flat :newAmount :: INT AS loan_amount,
        decoded_flat :newRate :: INT AS interest_rate_bps,
        interest_rate_bps / pow(
            10,
            4
        ) AS interest_rate,
        NULL AS offerhash,
        'refinance' AS event_type,
        _log_id,
        _inserted_timestamp
    FROM
        raw_logs
    WHERE
        event_name = 'Refinance'
),
borrows_combined_raw AS (
    SELECT
        *
    FROM
        loan_offer_taken_raw
    UNION ALL
    SELECT
        *
    FROM
        refinance_raw
),
borrows_combined_fill AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        decoded_flat,
        auction_duration_blocks,
        IFF(
            borrower_address IS NULL,
            LAG(borrower_address) ignore nulls over (
                PARTITION BY lienid
                ORDER BY
                    block_timestamp ASC,
                    event_index ASC
            ),
            borrower_address
        ) AS borrower_address,
        lender_address,
        lienid,
        nft_address,
        IFF(tokenId IS NULL, LAG(tokenId) ignore nulls over (PARTITION BY lienid
    ORDER BY
        block_timestamp ASC, event_index ASC), tokenId) AS tokenId,
        loan_amount,
        interest_rate,
        interest_rate_bps,
        IFF(offerhash IS NULL, LAG(offerhash) ignore nulls over (PARTITION BY lienid
    ORDER BY
        block_timestamp ASC, event_index ASC), offerhash) AS offerhash,
        event_type,
        IFF(
            event_type = 'refinance',
            2,
            1
        ) AS event_type_priority,
        _log_id,
        _inserted_timestamp
    FROM
        borrows_combined_raw
),
borrows_combined_qualify AS (
    SELECT
        *
    FROM
        borrows_combined_fill qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            lienid
            ORDER BY
                event_type_priority DESC
        ) = 1
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    'blur' AS platform_name,
    '0x29469395eaf6f95920e59f858042f0e28d98a20b' AS platform_address,
    'blend v1' AS platform_exchange_version,
    decoded_flat,
    auction_duration_blocks,
    borrower_address,
    lender_address,
    lienid,
    lienid AS loanid,
    nft_address,
    tokenId,
    loan_amount AS principal_unadj,
    NULL AS debt_unadj,
    '0x0000000000a39bb272e79075ade125fd351887ac' AS loan_token_address,
    interest_rate,
    interest_rate_bps,
    interest_rate_bps / pow(
        10,
        2
    ) AS interest_rate_percentage,
    interest_rate_percentage AS annual_percentage_rate,
    0 AS platform_fee_unadj,
    offerhash,
    event_type,
    event_type_priority,
    'perpetual' AS loan_term_type,
    block_timestamp AS loan_start_timestamp,
    NULL AS loan_tenure,
    NULL AS loan_due_timestamp,
    LAG(lender_address) over (
        PARTITION BY lienid
        ORDER BY
            block_timestamp ASC
    ) AS prev_lender_address,
    COALESCE(LAG(loan_amount) over (PARTITION BY lienid
ORDER BY
    block_timestamp ASC), 0) AS prev_principal_unadj,
    COALESCE(LAG(interest_rate) over (PARTITION BY lienid
ORDER BY
    block_timestamp ASC), 0) AS prev_interest_rate,
    LAG(block_timestamp) over (
        PARTITION BY lienid
        ORDER BY
            block_timestamp ASC
    ) AS prev_block_timestamp,
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
    borrows_combined_qualify
