{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['beacon']
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
    withdrawals,
    slot_json
FROM
    {{ ref('silver__beacon_blocks') }}
