{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "_log_id",
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
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_timestamp :: DATE >= '2023-05-01'
        AND contract_address = '0x29469395eaf6f95920e59f858042f0e28d98a20b'
        AND event_name IN (
            'LoanOfferTaken'
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
)
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
    raw_logs
