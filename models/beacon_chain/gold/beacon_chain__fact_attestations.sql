{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    slot_number,
    slot_timestamp,
    epoch_number,
    attestation_index,
    aggregation_bits,
    beacon_block_root,
    source_epoch,
    source_root,
    target_epoch,
    target_root,
    attestation_signature
FROM
    {{ ref('silver__beacon_attestations') }}
