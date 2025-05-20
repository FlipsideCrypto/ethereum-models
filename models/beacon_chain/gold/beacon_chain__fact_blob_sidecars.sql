{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['gold','beacon']
) }}

SELECT
    blob,
    blob_index,
    kzg_commitment,
    kzg_commitment_inclusion_proof,
    kzg_proof,
    body_root,
    parent_root,
    proposer_index,
    slot_number,
    state_root,
    signature,
    inserted_timestamp,
    modified_timestamp,
    blob_sidecar_id AS fact_blob_sidecar_id
FROM
    {{ ref('silver__blob_sidecars') }}
