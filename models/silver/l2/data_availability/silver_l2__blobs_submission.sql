{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','l2','curated']
) }}

WITH da_addresses AS (

    SELECT
        chain,
        chain_category,
        da_address,
        submission_type
    FROM
        {{ ref('silver_l2__da_address') }}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    from_address,
    to_address,
    chain,
    chain_category,
    e.to_address AS da_address,
    'blobs' AS submission_type,
    ARRAY_SIZE(blob_versioned_hashes) AS blob_count,
    blob_gas_used,
    blob_gas_price,
    blob_gas_used * blob_gas_price / pow(
        10,
        18
    ) AS blob_fee,
    inserted_timestamp
FROM
    {{ ref('core__fact_transactions') }}
    e
    LEFT JOIN da_addresses C
    ON e.to_address = C.da_address
    AND C.submission_type = 'blobs'
WHERE
    block_timestamp :: DATE >= '2024-03-01'
    AND tx_type = 3

{% if is_incremental() %}
AND inserted_timestamp >= (
    SELECT
        MAX(inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
