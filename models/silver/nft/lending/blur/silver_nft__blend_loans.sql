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
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2023-05-01'
        AND contract_address = '0x29469395eaf6f95920e59f858042f0e28d98a20b'
        AND event_name IN (
            'Refinance'
        )
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
loan_offer_taken_details AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        decoded_flat,
        auction_duration_blocks,
        borrower_address,
        lender_address,
        lienid,
        nft_address,
        tokenId,
        loan_amount,
        interest_rate_bps,
        interest_rate,
        offerhash,
        event_type,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_nft__blend_new_loans') }}

{% if is_incremental() %}
WHERE
    block_number IN (
        SELECT
            DISTINCT block_number
        FROM
            raw_logs
    )
    OR _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
    OR lienid NOT IN (
        SELECT
            lienid
        FROM
            {{ this }}
    )
{% endif %}
),
refinance_raw AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        decoded_log AS decoded_flat,
        decoded_flat :newAuctionDuration AS auction_duration_blocks,
        decoded_flat :newLender :: STRING AS lender_address,
        decoded_flat :lienId :: STRING AS lienid,
        decoded_flat :collection :: STRING AS nft_address,
        decoded_flat :newAmount :: INT AS loan_amount,
        decoded_flat :newRate :: INT AS interest_rate_bps,
        interest_rate_bps / pow(
            10,
            4
        ) AS interest_rate,
        NULL AS offerhash,
        'refinance' AS event_type,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        raw_logs
),
refinance_details AS (
    SELECT
        r.block_number,
        r.block_timestamp,
        r.tx_hash,
        r.event_index,
        r.event_name,
        r.decoded_flat,
        r.auction_duration_blocks,
        l.borrower_address,
        r.lender_address,
        r.lienid,
        r.nft_address,
        l.tokenId,
        r.loan_amount,
        r.interest_rate_bps,
        r.interest_rate,
        r.offerhash,
        r.event_type,
        r._log_id,
        r._inserted_timestamp
    FROM
        refinance_raw r
        INNER JOIN {{ ref('silver_nft__blend_new_loans') }}
        l USING (
            lienid,
            nft_address
        )
),
borrows_combined_raw AS (
    SELECT
        *
    FROM
        loan_offer_taken_details
    UNION ALL
    SELECT
        *
    FROM
        refinance_details
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
        borrower_address,
        lender_address,
        lienid,
        nft_address,
        tokenId,
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
    borrows_combined_fill
