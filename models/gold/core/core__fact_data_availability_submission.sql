{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_from_address,
    origin_to_address,
    chain,
    chain_category,
    data_availability_address,
    submission_type,
    COALESCE (
        complete_da_submission_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'chain','data_availability_address','submission_type']
        ) }}
    ) AS fact_data_availability_submission_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver_l2__complete_da_submission') }}
