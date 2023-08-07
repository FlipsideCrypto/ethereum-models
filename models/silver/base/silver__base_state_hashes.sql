{{ config(
    materialized = 'incremental',
    unique_key = "state_tx_hash",
    cluster_by = ['state_block_timestamp::DATE'],
    tags = ['base']
) }}

WITH base AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        contract_address,
        topics [1] :: STRING AS output_root,
        utils.udf_hex_to_int(
            topics [2] :: STRING
        ) :: INT AS batch_index,
        utils.udf_hex_to_int(
            topics [3] :: STRING
        ) :: INT AS min_l2_block_number,
        utils.udf_hex_to_int(
            DATA :: STRING
        ) :: TIMESTAMP AS l1_timestamp,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xa7aaf2512769da4e444e3de247be2564225c2e7a8f74cfe528e46e17d24868e2'
        AND contract_address = LOWER('0x56315b90c40730925ec5485cf004d835058518A0')

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
    CASE
        WHEN batch_index = 0 THEN 0
        ELSE min_l2_block_number - 1
    END AS state_prev_total_elements,
    min_l2_block_number - 1799 AS state_min_block,
    min_l2_block_number AS state_max_block,
    _inserted_timestamp
FROM
    base qualify(ROW_NUMBER() over(PARTITION BY state_tx_hash
ORDER BY
    _inserted_timestamp DESC)) = 1
