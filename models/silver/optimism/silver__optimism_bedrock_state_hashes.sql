{{ config(
    materialized = 'incremental',
    unique_key = "state_tx_hash",
    cluster_by = ['state_block_timestamp::DATE'],
    tags = ['optimism']
) }}

WITH base AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        decoded_flat: outputRoot :: STRING AS output_root,
        decoded_flat: l2OutputIndex :: INT AS batch_index,
        decoded_flat: l2BlockNumber :: INT AS min_l2_block_number,
        decoded_flat: l1Timestamp :: TIMESTAMP AS l1_timestamp,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        origin_to_address = '0xdfe97868233d1aa22e815a266982f2cf17685a27'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    tx_hash AS state_tx_hash,
    block_number AS state_block_number,
    block_timestamp AS state_block_timestamp,
    batch_index AS state_batch_index,
    output_root AS state_batch_root,
    1800 AS state_batch_size,
    min_l2_block_number - 2 AS state_prev_total_elements,
    min_l2_block_number AS state_min_block,
    min_l2_block_number + 1799 AS state_max_block,
    _inserted_timestamp
FROM
    base qualify(ROW_NUMBER() over(PARTITION BY state_tx_hash
ORDER BY
    _inserted_timestamp DESC)) = 1