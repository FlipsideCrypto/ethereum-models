{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','curated','maker']
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
        modified_timestamp AS _inserted_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
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
        ) = '0x6111be2e'
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
        utils.udf_hex_to_string(REPLACE(segmented_data [2] :: STRING, '0x', '')) AS ilk_l,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS receiver,
        CONCAT('0x', SUBSTR(segmented_data [4] :: STRING, 25, 40)) AS sender,
        utils.udf_hex_to_int(
            segmented_data [5] :: STRING
        ) / pow(
            10,
            18
        ) AS wad,
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
    SUBSTR(ilk_l, 0, POSITION('-', ilk_l) + 1) AS ilk,
    receiver,
    sender,
    wad,
    _inserted_timestamp,
    _log_id,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS vat_flux_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
