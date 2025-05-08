{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated']
) }}

WITH da_addresses AS (

    SELECT
        chain,
        chain_category,
        da_address,
        submission_type
    FROM
        {{ ref('silver_l2__da_address') }}
),
non_blob_transactions AS (
    SELECT
        tx_hash,
        tx_type
    FROM
        {{ ref('core__fact_transactions') }}
    WHERE
        block_timestamp :: DATE >= '2021-01-01'
        AND tx_type != 3

{% if is_incremental() %}
AND inserted_timestamp >= (
    SELECT
        MAX(inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    event_index,
    e.tx_hash,
    origin_from_address,
    origin_to_address,
    contract_address,
    topic_0,
    chain,
    chain_category,
    da_address,
    submission_type,
    inserted_timestamp
FROM
    {{ ref('core__fact_event_logs') }}
    e
    INNER JOIN da_addresses C
    ON e.contract_address = C.da_address
    AND C.submission_type = 'calldata'
    INNER JOIN non_blob_transactions USING (tx_hash)
WHERE
    block_timestamp :: DATE >= '2021-01-01'
    AND topic_0 IN (
        '0x10e0571aafaf282151fd5b0215b5495521c549509cb0de3a3f8310bd2e344682',
        -- SequencerBatchDeliveredFromOrigin, old arbitrum
        '0x7394f4a19a13c7b92b5bb71033245305946ef78452f7b4986ac1390b5df4ebd7',
        -- SequencerBatchDelivered, new arbitrum
        '0x127186556e7be68c7e31263195225b4de02820707889540969f62c05cf73525e' -- TransactionBatchAppended, op_stack
    )
    AND NOT EXISTS (
        SELECT
            1
        FROM
            non_blob_transactions t
        WHERE
            t.tx_hash = e.tx_hash
    )

{% if is_incremental() %}
AND inserted_timestamp >= (
    SELECT
        MAX(inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
