{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime']
) }}

WITH pool_name AS (

    SELECT
        pool_name,
        pool_address
    FROM
        {{ ref('silver_dex__balancer_pools') }}
),
swaps_base AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        _inserted_timestamp,
        'Swap' AS event_name,
        event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        (
            CASE
                WHEN segmented_data [0] = '0x' THEN NULL
                ELSE utils.udf_hex_to_int(
                    segmented_data [0] :: STRING
                )
            END
        ) :: INTEGER AS amount_in_unadj,
        (
            CASE
                WHEN segmented_data [1] = '0x' THEN NULL
                ELSE utils.udf_hex_to_int(
                    segmented_data [1] :: STRING
                )
            END
        ) :: INTEGER AS amount_out_unadj,
        topics [1] :: STRING AS pool_id,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token_in,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS token_out,
        SUBSTR(
            topics [1] :: STRING,
            1,
            42
        ) AS pool_address,
        _log_id,
        'balancer' AS platform,
        origin_from_address AS sender,
        origin_from_address AS tx_to
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x2170c741c41531aec20e7c107c24eecfdd15e69c9bb0a8dd37b1840b9e0b207b'
        AND contract_address = '0xba12222222228d8ba445958a75a0704d566bf2c8'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    _inserted_timestamp,
    event_name,
    event_index,
    amount_in_unadj,
    amount_out_unadj,
    pool_id,
    token_in,
    token_out,
    s.pool_address AS contract_address,
    _log_id,
    platform,
    sender,
    tx_to,
    pool_name
FROM
    swaps_base s
    INNER JOIN pool_name pn
    ON pn.pool_address = s.pool_address
