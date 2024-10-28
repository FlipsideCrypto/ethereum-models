{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    from_address,
    to_address,
    origin_function_signature,
    VALUE,
    value_precise_raw,
    value_precise,
    tx_fee,
    tx_fee_precise,
    CASE
        WHEN tx_status = 'SUCCESS' THEN TRUE
        ELSE FALSE
    END AS tx_succeeded, --new column
    tx_type,
    nonce,
    POSITION AS tx_position, --new column
    input_data,
    gas_price,
    gas_used,
    gas AS gas_limit,
    cumulative_gas_used,
    effective_gas_price,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    blob_versioned_hashes,
    max_fee_per_blob_gas,
    blob_gas_used,
    blob_gas_price,
    r,
    s,
    v,
    COALESCE (
        transactions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash']
        ) }}
    ) AS fact_transactions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp,
    block_hash, --deprecate
    POSITION, --deprecate
    tx_status AS status, --deprecate
    chain_id --deprecate
FROM
    {{ ref('silver__transactions') }}
