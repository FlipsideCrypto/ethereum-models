{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH calculation_helpers AS (

    SELECT
        *
    FROM
        (
            VALUES
                ('0x5c55cd67a6bd0d4c315b50cb6cd589bfb080017e'),
                ('0xacd3d255b93f6f91dd6311ae9a30cb256e118cf2'),
                ('0x5c06da8ec7945c05304d79895046d01a7943826d')
        ) t (helpers)
),
repay_txs AS (
    SELECT
        tx_hash,
        event_name,
        event_index,
        decoded_flat
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_timestamp :: DATE >= '2023-05-01'
        AND contract_address = '0x29469395eaf6f95920e59f858042f0e28d98a20b'
        AND event_name IN (
            'Repay',
            'Refinance'
        )
),
traces_raw AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        trace_index,
        from_address,
        to_address,
        input,
        output,
        LEFT(
            input,
            10
        ) AS function_sig,
        regexp_substr_all(SUBSTR(input, 11), '.{64}') AS segmented_data,
        segmented_data [0],
        utils.udf_hex_to_int(output) AS debt_with_interest,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS initial_loan_amount,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) AS rate,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) AS start_time
    FROM
        {{ ref('silver__traces') }}
    WHERE
        block_timestamp >= '2023-05-01'
        AND TYPE = 'DELEGATECALL'
        AND trace_status = 'SUCCESS'
        AND from_address = '0x29469395eaf6f95920e59f858042f0e28d98a20b'
        AND to_address IN (
            SELECT
                helpers
            FROM
                calculation_helpers
        )
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                repay_txs
        )
        AND LEFT(
            input,
            10
        ) = '0x1b70b278' --computeCurrentDebt
),
repay_lien AS (
    SELECT
        tx_hash,
        event_index,
        decoded_flat :collection :: STRING AS nft_address,
        decoded_flat :lienId AS lienid
    FROM
        repay_txs
    WHERE
        event_name IN ('Repay')
),
all_loans AS (
    SELECT
        block_timestamp,
        tx_hash,
        nft_address,
        lienid,
        borrower_address,
        tokenid,
        category {# prev_lender_address,
        prev_loan_amount,
        prev_interest_rate #}
    FROM
        {{ ref('silver_nft__blur_loans_taken') }}
),
refinance_lien AS (
    SELECT
        tx_hash,
        nft_address,
        lienid {# prev_lender_address,
        prev_loan_amount,
        prev_interest_rate #}
    FROM
        all_loans
    WHERE
        category = 'refinance'
),
repayment_base AS (
    SELECT
        block_number,
        block_timestamp,
        r.tx_hash,
        debt_with_interest,
        initial_loan_amount,
        rate,
        start_time,
        p.lienid AS repay_lienid,
        f.lienid AS refinance_lienid,
        COALESCE(
            p.lienid,
            f.lienid
        ) AS lienid,
        COALESCE(
            p.nft_address,
            f.nft_address
        ) AS nft_address_final,
        -- prev_lender_address,
        -- prev_loan_amount,
        -- prev_interest_rate,
        IFF(
            repay_lienid IS NOT NULL,
            'repay',
            'refinance'
        ) AS TYPE
    FROM
        traces_raw r
        LEFT JOIN repay_lien p USING (tx_hash)
        LEFT JOIN refinance_lien f USING (tx_hash)
),
lien_details AS (
    SELECT
        lienid,
        borrower_address,
        nft_address,
        tokenid
    FROM
        all_loans qualify ROW_NUMBER() over (
            PARTITION BY lienid
            ORDER BY
                block_timestamp ASC
        ) = 1
)
SELECT
    *
FROM
    repayment_base
    LEFT JOIN lien_details USING (lienid)
