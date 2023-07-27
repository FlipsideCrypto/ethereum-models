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
        event_index,
        origin_from_address,
        origin_to_address,
        _inserted_timestamp,
        _log_id,
        contract_address
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_number > 8000000
        AND contract_address = '0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b'
        AND LEFT(
            topics [0] :: STRING,
            10
        ) = '0xf24e23eb'
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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS u_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS v_address,
        utils.udf_hex_to_int(
            topics [3] :: STRING
        ) / pow(
            10,
            45
        ) AS rad,
        contract_address
    FROM
        base
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    origin_from_address,
    origin_to_address,
    u_address,
    v_address,
    rad,
    _inserted_timestamp,
    _log_id
FROM
    FINAL
