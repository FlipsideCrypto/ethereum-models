{{ config(
    materialized = 'incremental',
    unique_key = "tx_hash",
    cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
    tags = ['core'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    incremental_strategy = 'delete+insert'
) }}

WITH base_txs AS (

    SELECT
        record_id,
        tx_id,
        tx_block_index,
        offset_id,
        block_id,
        block_timestamp,
        network,
        chain_id,
        tx,
        ingested_at,
        _inserted_timestamp
    FROM
        {{ ref('bronze__transactions') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
),
logs_raw AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id AS tx_hash,
        tx :receipt :logs AS full_logs,
        ingested_at :: TIMESTAMP AS ingested_at,
        _inserted_timestamp :: TIMESTAMP AS _inserted_timestamp,
        CASE
            WHEN tx :receipt :status :: STRING = '0x1' THEN 'SUCCESS'
            ELSE 'FAIL'
        END AS tx_status,
        SUBSTR(
            tx :input :: STRING,
            1,
            10
        ) AS origin_function_signature,
        tx :from :: STRING AS origin_from_address,
        tx :to :: STRING AS origin_to_address
    FROM
        base_txs
),
logs AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_status,
        ingested_at,
        _inserted_timestamp,
        silver.js_hex_to_int(
            VALUE :logIndex :: STRING
        ) AS event_index,
        VALUE :address :: STRING AS contract_address,
        VALUE :decoded :contractName :: STRING AS contract_name,
        VALUE :decoded :eventName :: STRING AS event_name,
        VALUE :decoded :inputs :: OBJECT AS event_inputs,
        VALUE :topics AS topics,
        VALUE :data :: STRING AS DATA,
        VALUE :removed :: STRING AS event_removed
    FROM
        logs_raw,
        LATERAL FLATTEN (
            input => full_logs
        )
)
SELECT
    concat_ws(
        '-',
        tx_hash,
        event_index
    ) AS _log_id,
    block_id AS block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    ingested_at,
    _inserted_timestamp,
    event_index,
    contract_address,
    contract_name,
    event_name,
    event_inputs,
    topics,
    DATA,
    event_removed,
    tx_status
FROM
    logs qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
