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
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        event_index,
        origin_from_address,
        origin_to_address,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_number > 8000000
        AND contract_address = '0x135954d155898d42c90d2a57824c690e0c7bef1b'
        AND LEFT(
            topics [0] :: STRING,
            10
        ) = '0x85258d09'
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
        TRY_HEX_DECODE_STRING(REPLACE(topics [1] :: STRING, '0x', '')) AS ilk_l,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS urn_address,
        PUBLIC.udf_hex_to_int(
            topics [3] :: STRING
        ) AS id,
        PUBLIC.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS ink,
        PUBLIC.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) AS art,
        PUBLIC.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) / pow(
            10,
            45
        ) AS due,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS clip,
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
    SUBSTR(ilk_l, 0, POSITION('-', ilk_l) + 1) AS ilk,
    urn_address,
    art,
    ink,
    due,
    clip,
    id,
    _inserted_timestamp,
    _log_id
FROM
    FINAL
