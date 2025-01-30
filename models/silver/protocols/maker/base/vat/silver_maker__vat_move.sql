{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
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
        modified_timestamp AS _inserted_timestamp,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        contract_address
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_number > 8000000
        AND contract_address = '0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b'
        AND LEFT(
            topics [0] :: STRING,
            10
        ) = '0xbb35783b'
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS src_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS dst_address,
        utils.udf_hex_to_int(
            's2c',
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
    src_address,
    dst_address,
    rad,
    _inserted_timestamp,
    _log_id,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS vat_move_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
