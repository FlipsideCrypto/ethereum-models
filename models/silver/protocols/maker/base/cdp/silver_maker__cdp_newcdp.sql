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
        AND contract_address = '0x5ef30b9986345249bc32d8928b7ee64de9435e39'
        AND LEFT(
            topics [0] :: STRING,
            10
        ) = '0xd6be0bc1'
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
        tx_hash,
        event_index,
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS usr,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS own,
        utils.udf_hex_to_int(
            topics [3] :: STRING
        ) :: INTEGER AS cdp,
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
    own,
    cdp,
    _inserted_timestamp,
    _log_id,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS cdp_newcdp_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
