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
        AND contract_address = '0xa5679c04fc3d9d8b0aab1f0ab83555b301ca70ea'
        AND LEFT(
            topics [0] :: STRING,
            10
        ) = '0xa716da86'
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
        TRY_HEX_DECODE_STRING(REPLACE(topics [1] :: STRING, '0x', '')) AS ilk,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS urn_address,
        PUBLIC.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) / pow(
            10,
            18
        ) AS ink,
        PUBLIC.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) / pow(
            10,
            18
        ) AS art,
        PUBLIC.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) / pow(
            10,
            45
        ) AS tab,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS flip,
        PUBLIC.udf_hex_to_int(
            segmented_data [4] :: STRING
        ) AS id,
        _inserted_timestamp,
        _log_id
    FROM
        base
)
SELECT
    tx_hash,
    event_index,
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    ilk,
    urn_address,
    art,
    ink,
    tab,
    id,
    _inserted_timestamp,
    _log_id
FROM
    FINAL
