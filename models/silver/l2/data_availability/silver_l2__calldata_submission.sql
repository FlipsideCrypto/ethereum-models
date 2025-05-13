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
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    from_address,
    to_address,
    chain,
    chain_category,
    da_address,
    submission_type,
    inserted_timestamp
FROM
    {{ ref('core__fact_transactions') }}
    t
    INNER JOIN da_addresses C
    ON t.to_address = C.da_address
    AND C.submission_type = 'calldata'
WHERE
    block_timestamp :: DATE >= '2021-01-01'
    AND tx_type IN (
        0,
        1,
        2
    )
    AND input_data != '0x'

{% if is_incremental() %}
AND inserted_timestamp >= (
    SELECT
        MAX(inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
