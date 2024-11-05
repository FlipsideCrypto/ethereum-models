{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH raw_logs AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        decoded_log AS decoded_flat,
        decoded_flat :lienId :: STRING AS lienid,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2023-05-01'
        AND contract_address = '0x29469395eaf6f95920e59f858042f0e28d98a20b'
        AND event_name IN (
            'LoanOfferTaken',
            'Refinance'
        )
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
filtered_loan_events AS (
    SELECT
        tx_hash,
        lienid,
        COUNT(1) AS new_loan_filter
    FROM
        raw_logs
    GROUP BY
        tx_hash,
        lienid
    HAVING
        new_loan_filter = 1
),
base AS (
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
        lienid,
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
        lienid IN (
            SELECT
                lienid
            FROM
                filtered_loan_events
        )
),
FINAL AS (
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
        base

{% if is_incremental() %}
UNION ALL
SELECT
    *
FROM
    {{ this }}
{% endif %}
)
SELECT
    *
FROM
    FINAL qualify ROW_NUMBER() over (
        PARTITION BY lienid
        ORDER BY
            block_timestamp ASC,
            event_index ASC
    ) = 1
