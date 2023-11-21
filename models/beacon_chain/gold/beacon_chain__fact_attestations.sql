{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    slot_number,
    slot_timestamp,
    epoch_number,
    attestation_slot,
    attestation_index,
    aggregation_bits,
    beacon_block_root,
    source_epoch,
    source_root,
    target_epoch,
    target_root,
    attestation_signature,
    COALESCE (
       beacon_attestation_id,
        {{ dbt_utils.generate_surrogate_key(
            ['slot_number', 'attestation_slot', 'attestation_index', 'aggregation_bits', 'beacon_block_root', 'attestation_signature']
        ) }}
    ) AS fact_attestations_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__beacon_attestations') }}
