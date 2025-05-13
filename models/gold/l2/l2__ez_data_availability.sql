{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number'],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, chain, chain_category)",
    tags = ['curated','reorg']
) }}

WITH base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        chain,
        chain_category,
        da_address,
        submission_type,
        NULL AS blob_count,
        NULL AS blob_gas_used,
        NULL AS blob_gas_price,
        NULL AS blob_fee,
        inserted_timestamp
    FROM
        {{ ref('silver_l2__calldata_submission') }}

{% if is_incremental() and 'calldata' not in var('HEAL_MODELS') %}
WHERE
    inserted_timestamp >= (
        SELECT
            MAX(inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    from_address AS origin_from_address,
    to_address AS origin_to_address,
    chain,
    chain_category,
    da_address,
    submission_type,
    blob_count,
    blob_gas_used,
    blob_gas_price,
    blob_fee,
    inserted_timestamp
FROM
    {{ ref('silver_l2__blobs_submission') }}

{% if is_incremental() and 'blobs' not in var('HEAL_MODELS') %}
WHERE
    inserted_timestamp >= (
        SELECT
            MAX(inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_from_address,
    origin_to_address,
    chain,
    chain_category,
    da_address AS data_availability_address,
    submission_type,
    blob_count,
    blob_gas_used,
    blob_gas_price,
    blob_fee,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'chain','data_availability_address','submission_type']
    ) }} AS fact_data_availability_submission_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    base
