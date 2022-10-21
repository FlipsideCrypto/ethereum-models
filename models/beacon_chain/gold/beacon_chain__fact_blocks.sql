{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    slot_number,
    epoch_number,
    slot_timestamp,
    proposer_index,
    parent_root,
    state_root,
    randao_reveal,
    graffiti,
    eth1_block_hash,
    eth1_deposit_count,
    eth1_deposit_root,
    execution_payload,
    signature,
    attester_slashings,
    proposer_slashings,
    deposits,
    attestations,
    slot_json,
    DATA AS block_json
FROM
    {{ ref('silver__beacon_blocks') }}
