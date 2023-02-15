{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        topics,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 11, len(DATA)), '.{64}') AS segmented_data,
        event_index,
        origin_from_address,
        origin_to_address,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_number > 8000000
        AND contract_address = '0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b'
        AND LEFT(
            topics [0] :: STRING,
            10
        ) = '0x76088703'
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        _inserted_timestamp,
        _log_id,
        TRIM(
            TRY_HEX_DECODE_STRING(
                segmented_data [2] :: STRING
            )
        ) :: STRING AS ilk,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS u_address,
        CONCAT('0x', SUBSTR(segmented_data [4] :: STRING, 25, 40)) AS v_address,
        CONCAT('0x', SUBSTR(segmented_data [5] :: STRING, 25, 40)) AS w_address,
        ethereum.public.udf_hex_to_int(
            's2c',
            segmented_data [6] :: STRING
        ) / pow(
            10,
            18
        ) AS dink,
        ethereum.public.udf_hex_to_int(
            's2c',
            segmented_data [7] :: STRING
        ) / pow(
            10,
            18
        ) AS dart
    FROM
        base
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    ilk,
    u_address,
    v_address,
    w_address,
    dink,
    dart,
    _inserted_timestamp,
    _log_id
FROM
    FINAL
