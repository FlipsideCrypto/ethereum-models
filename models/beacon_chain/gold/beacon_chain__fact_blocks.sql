{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['gold','beacon']
) }}

SELECT
    execution_payload :block_number :: INT AS block_number,
    slot_number,
    epoch_number,
    IFF(
        slot_timestamp IS NULL
        OR slot_timestamp < '2022-12-01',
        DATEADD(
            'seconds',
            slot_number * 12,
            '2020-12-01T12:00:23Z' :: timestamp_ntz
        ),
        slot_timestamp
    ) :: timestamp_ntz slot_timestamp,
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
    slot_json,
    blob_kzg_commitments,
    blob_gas_used,
    excess_blob_gas,
    block_included,
    COALESCE (
        beacon_blocks_id,
        {{ dbt_utils.generate_surrogate_key(
            ['slot_number']
        ) }}
    ) AS fact_blocks_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__beacon_blocks') }}
