{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
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
        regexp_substr_all(SUBSTR(DATA, 11, len(DATA)), '.{64}') AS segmented_data,
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
        AND contract_address = '0x5ef30b9986345249bc32d8928b7ee64de9435e39'
        AND LEFT(
            topics [0] :: STRING,
            10
        ) = '0x45e6bdcd'
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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS usr,
        utils.udf_hex_to_int(
            topics [2] :: STRING
        ) :: INTEGER AS cdp,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [3] :: STRING
        ) / pow(
            10,
            18
        ) AS dink,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [4] :: STRING
        ) / pow(
            10,
            18
        ) AS dart,
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
    usr,
    cdp,
    dink,
    dart,
    _inserted_timestamp,
    _log_id
FROM
    FINAL
