{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    event_index,
    tx_hash,
    origin_from_address,
    origin_to_address,
    contract_address,
    chain,
    chain_category,
    validation_address,
    validation_type,
    validation_data,
    validation_data_type,
    COALESCE(
        complete_state_validation_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index', 'chain','validation_type']
        ) }}
    ) AS fact_state_validation_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver_l2__complete_state_validation') }}
