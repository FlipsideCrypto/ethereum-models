{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
    tags = ['core']
) }}

WITH logs AS (

    SELECT
        _log_id,
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_name,
        event_index,
        event_inputs,
        topics,
        DATA,
        ingested_at :: TIMESTAMP AS ingested_at,
        _inserted_timestamp :: TIMESTAMP AS _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        tx_status = 'SUCCESS'

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
),
transfers AS (
    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address :: STRING AS contract_address,
        event_inputs :from :: STRING AS from_address,
        event_inputs :to :: STRING AS to_address,
        event_inputs :value :: FLOAT AS raw_amount,
        event_index,
        ingested_at,
        _inserted_timestamp
    FROM
        logs
    WHERE
        event_name = 'Transfer'
        AND raw_amount IS NOT NULL
),
find_missing_events AS (
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
        COALESCE(udf_hex_to_int(topics [3] :: STRING), udf_hex_to_int(SUBSTR(DATA, 3, 64))) :: FLOAT AS raw_amount,
        event_index,
        ingested_at,
        _inserted_timestamp
    FROM
        logs
    WHERE
        event_name IS NULL
        AND contract_address IN (
            SELECT
                DISTINCT contract_address
            FROM
                transfers
        )
        AND topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
),
all_transfers AS (
    SELECT
        _log_id,
        tx_hash,
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        from_address,
        to_address,
        raw_amount,
        event_index,
        ingested_at,
        _inserted_timestamp
    FROM
        transfers
    UNION ALL
    SELECT
        _log_id,
        tx_hash,
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        from_address,
        to_address,
        raw_amount,
        event_index,
        ingested_at,
        _inserted_timestamp
    FROM
        find_missing_events
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
    ingested_at,
    _inserted_timestamp,
    event_index
FROM
    all_transfers qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
