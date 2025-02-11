{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "l1_submission_block_number",
    cluster_by = ['l1_submission_block_timestamp::DATE'],
    tags = ['optimism','curated']
) }}

WITH base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        topics,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        event_removed,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_succeeded,
        fact_event_logs_id,
        inserted_timestamp,
        modified_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int(
            topics [1] :: STRING
        ) :: INTEGER AS batch_index,
        CONCAT(
            '0x',
            segmented_data [0] :: STRING
        ) AS batchRoot,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS batchSize,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INTEGER AS prevTotalElements,
        segmented_data [3] :: STRING AS extraData
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        origin_from_address = '0x6887246668a3b87f54deb3b94ba47a6f63f32985'
        AND origin_to_address = '0x5e4e65926ba27467555eb562121fac00d24e9dd2'
        AND topics [0] :: STRING = '0x127186556e7be68c7e31263195225b4de02820707889540969f62c05cf73525e'
        AND tx_succeeded
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
