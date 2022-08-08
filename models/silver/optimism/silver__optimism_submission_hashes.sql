{{ config(
    materialized = 'incremental',
    unique_key = "l1_submission_tx_hash",
    cluster_by = ['l1_submission_block_timestamp::DATE'],
    tags = ['optimism']
) }}

WITH base AS (

    SELECT
        *,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        PUBLIC.udf_hex_to_int(
            topics [1] :: STRING
        ) :: INTEGER AS batch_index,
        CONCAT(
            '0x',
            segmented_data [0] :: STRING
        ) AS batchRoot,
        PUBLIC.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS batchSize,
        PUBLIC.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INTEGER AS prevTotalElements,
        segmented_data [3] :: STRING AS extraData
    FROM
        {{ ref('silver__logs') }}
    WHERE
        origin_from_address = '0x6887246668a3b87f54deb3b94ba47a6f63f32985'
        AND origin_to_address = '0x5e4e65926ba27467555eb562121fac00d24e9dd2'
        AND topics [0] :: STRING = '0x127186556e7be68c7e31263195225b4de02820707889540969f62c05cf73525e'
        AND tx_status = 'SUCCESS'
        AND event_removed = 'false'

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
    tx_hash AS l1_submission_tx_hash,
    block_number AS l1_submission_block_number,
    block_timestamp AS l1_submission_block_timestamp,
    batch_index AS l1_submission_batch_index,
    batchRoot AS l1_submission_batch_root,
    batchSize AS l1_submission_size,
    prevTotalElements AS l1_submission_prev_total_elements,
    l1_submission_prev_total_elements + 2 AS sub_min_block,
    l1_submission_prev_total_elements + 1 + l1_submission_size AS sub_max_block,
    _inserted_timestamp
FROM
    base qualify(ROW_NUMBER() over(PARTITION BY l1_submission_tx_hash
ORDER BY
    _inserted_timestamp DESC)) = 1
