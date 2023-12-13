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
        decoded_flat :rate :: INT AS interest_rate,
        interest_rate / pow(
            10,
            4
        ) AS interest_rate_percent,
        decoded_flat :offerHash :: STRING AS offerhash,
        'loan offer taken' AS category,
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
        decoded_flat :newRate :: INT AS interest_rate,
        interest_rate / pow(
            10,
            4
        ) AS interest_rate_percent,
        NULL AS offerhash,
        'refinance' AS category,
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
        interest_rate_percent,
        IFF(offerhash IS NULL, LAG(offerhash) ignore nulls over (PARTITION BY lienid
    ORDER BY
        block_timestamp ASC, event_index ASC), offerhash) AS offerhash,
        category,
        IFF(
            category = 'refinance',
            2,
            1
        ) AS category_priority,
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
                category_priority DESC
        ) = 1
)
SELECT
    *,
    LAG(lender_address) over (
        PARTITION BY lienid
        ORDER BY
            block_timestamp ASC
    ) AS prev_lender_address,
    COALESCE(LAG(loan_amount) over (PARTITION BY lienid
ORDER BY
    block_timestamp ASC), 0) AS prev_loan_amount,
    COALESCE(LAG(interest_rate) over (PARTITION BY lienid
ORDER BY
    block_timestamp ASC), 0) AS prev_interest_rate
FROM
    borrows_combined_qualify
