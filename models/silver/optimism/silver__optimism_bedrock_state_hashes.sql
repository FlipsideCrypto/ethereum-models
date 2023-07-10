{{ config(
    materialized = 'incremental',
    unique_key = "state_tx_hash",
    cluster_by = ['state_block_timestamp::DATE'],
    tags = ['optimism']
) }}

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
