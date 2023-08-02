{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
    tags = ['core']
) }}

WITH logs AS (

    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address :: STRING AS contract_address,
        CONCAT('0x', SUBSTR(topics [1], 27, 40)) :: STRING AS from_address,
        CONCAT('0x', SUBSTR(topics [2], 27, 40)) :: STRING AS to_address,
        utils.udf_hex_to_int(SUBSTR(DATA, 3, 64)) AS raw_amount_precise,
        raw_amount_precise :: FLOAT AS raw_amount,
        event_index :: FLOAT AS event_index,
        TO_TIMESTAMP_NTZ(_inserted_timestamp) AS _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    _log_id,
    block_number,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    block_timestamp,
    contract_address,
    from_address,
    to_address,
    raw_amount,
    _inserted_timestamp,
    event_index,
    raw_amount_precise
FROM
    logs
WHERE
    raw_amount IS NOT NULL
    AND to_address IS NOT NULL
    AND from_address IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
