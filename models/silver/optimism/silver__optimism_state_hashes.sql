{{ config(
    materialized = 'incremental',
    unique_key = "state_tx_hash",
    cluster_by = ['state_block_timestamp::DATE'],
    tags = ['optimism','non_realtime']
) }}

WITH base AS (

    SELECT
        *,
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
        {{ ref('silver__logs') }}
    WHERE
        origin_from_address = '0x473300df21d047806a082244b417f96b32f13a33'
        AND origin_to_address = '0xbe5dab4a2e9cd0f27300db4ab94bee3a233aeb19'
        AND topics [0] :: STRING = '0x16be4c5129a4e03cf3350262e181dc02ddfb4a6008d925368c0899fcd97ca9c5'
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
    state_tx_hash,
    state_block_number,
    state_block_timestamp,
    state_batch_index AS bedrock_state_batch_index,
    state_batch_root AS bedrock_state_batch_root,
    NULL AS state_batch_index,
    NULL AS state_batch_root,
    state_batch_size,
    state_prev_total_elements,
    state_min_block,
    state_max_block,
    _inserted_timestamp
FROM
    {{ ref('silver__optimism_bedrock_state_hashes') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
             {{ this }}
    )
{% endif %}
UNION
SELECT
    tx_hash AS state_tx_hash,
    block_number AS state_block_number,
    block_timestamp AS state_block_timestamp,
    NULL AS bedrock_state_batch_index,
    NULL AS bedrock_state_batch_root,
    batch_index AS state_batch_index,
    batchRoot AS state_batch_root,
    batchSize AS state_batch_size,
    prevTotalElements AS state_prev_total_elements,
    state_prev_total_elements + 2 AS state_min_block,
    state_prev_total_elements + 1 + state_batch_size AS state_max_block,
    _inserted_timestamp
FROM
    base qualify(ROW_NUMBER() over(PARTITION BY state_tx_hash
ORDER BY
    _inserted_timestamp DESC)) = 1
