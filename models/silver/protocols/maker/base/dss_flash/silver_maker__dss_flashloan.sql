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
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        event_index,
        origin_from_address,
        origin_to_address,
        _inserted_timestamp,
        _log_id,
        contract_address
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_number > 8000000
        AND contract_address IN (
            '0x1eb4cf3a948e7d72a198fe073ccb8c7a948cd853',
            '0x60744434d6339a6b27d73d9eda62b6f66a0a04fa'
        )
        AND LEFT(
            topics [0] :: STRING,
            10
        ) = '0x0d7d75e0'
        AND tx_succeeded

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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS receiver,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS token,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER / pow(
            10,
            18
        ) AS amount,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INTEGER / pow(
            10,
            18
        ) AS fee,
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
    receiver,
    token,
    amount,
    fee,
    _inserted_timestamp,
    _log_id,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS dss_flashloan_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
