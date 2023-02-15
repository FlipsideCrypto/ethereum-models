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
        ) = '0x1a0b287e'
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
        TRY_HEX_DECODE_STRING(REPLACE(topics [1] :: STRING, '0x', '')) AS ilk_l,
        TRY_HEX_DECODE_STRING(REPLACE(topics [2] :: STRING, '0x', '')) AS what,
        PUBLIC.udf_hex_to_int(
            topics [3] :: STRING
        ) AS DATA
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
    SUBSTR(ilk_l, 0, POSITION('-', ilk_l) + 1) AS ilk,
    what,
    DATA,
    _inserted_timestamp,
    _log_id
FROM
    FINAL
