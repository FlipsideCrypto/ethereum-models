{{ config(
    materialized = 'incremental',
    unique_key = "state_tx_hash",
    cluster_by = ['state_block_timestamp::DATE'],
    tags = ['optimism']
) }}
with base as (
    select
        tx_hash,
        block_number,
        decoded_flat: outputRoot:: string as output_root,
        decoded_flat: l2OutputIndex:: int as batch_index,
        decoded_flat: l2BlockNumber:: int as l2_block_number,
        decoded_flat: l1Timestamp:: timestamp as l1_timestamp,
        _inserted_timestamp
    from 
        {{ ref('silver__decoded_logs') }}
    where 
        ORIGIN_TO_ADDRESS = '0xdfe97868233d1aa22e815a266982f2cf17685a27'
)

select
    tx_hash as state_tx_hash,
    block_number as state_block_number,
    block_timestamp as state_block_timestamp,
    batch_index as state_batch_index,
    output_root as state_batch_root,
    l2_block_number,
    l1_timestamp,
    _inserted_timestamp
FROM
    base qualify(ROW_NUMBER() over(PARTITION BY state_tx_hash
ORDER BY
    _inserted_timestamp DESC)) = 1

