{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime']
) }}

WITH base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        topics,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
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
        AND contract_address = '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7'
        AND LEFT(
            topics [0] :: STRING,
            10
        ) = '0x7f8661a1'
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
        tx_hash,
        event_index,
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) / pow(
            10,
            18
        ) AS wad,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS usr,
        _inserted_timestamp,
        _log_id,
        contract_address
    FROM
        base
)
SELECT
    tx_hash,
    event_index,
    block_number,
    block_timestamp,
    contract_address,
    origin_from_address,
    origin_to_address,
    wad,
    usr,
    _inserted_timestamp,
    _log_id
FROM
    FINAL
