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
        AND contract_address = '0x9759a6ac90977b93b58547b4a71c78317f391a28'
        AND LEFT(
            topics [0] :: STRING,
            10
        ) = '0x3b4da69f'
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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS usr1,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS usr2,
        utils.udf_hex_to_int(
            topics [3] :: STRING
        ) :: INTEGER / pow(
            10,
            18
        ) AS wad,
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
    usr1,
    usr2,
    wad,
    _inserted_timestamp,
    _log_id
FROM
    FINAL