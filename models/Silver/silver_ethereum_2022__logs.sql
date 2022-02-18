{{ config(
    materialized = 'incremental',
    unique_key = "log_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['ingested_at::DATE'],
    tags = ['snowflake', 'ethereum', 'silver_ethereum', 'ethereum_logs']
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
        ingested_at
    FROM
        {{ ref('bronze_ethereum_2022__transactions') }}

{% if is_incremental() %}
WHERE
    ingested_at >= (
        SELECT
            MAX(
                ingested_at
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
        ingested_at :: TIMESTAMP AS ingested_at
    FROM
        base_txs
),
logs AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        ingested_at,
        silver_ethereum_2022.js_hex_to_int(
            VALUE :logIndex :: STRING
        ) AS event_index,
        VALUE :address :: STRING AS contract_address,
        VALUE :decoded :contractName :: STRING AS contract_name,
        VALUE :decoded :eventName :: STRING AS event_name,
        VALUE :decoded :inputs AS event_inputs,
        VALUE :topics AS topics,
        VALUE :data :: STRING AS DATA,
        VALUE :removed AS event_removed
    FROM
        logs_raw,
        LATERAL FLATTEN (
            input => full_logs
        )
),
FINAL AS (
    SELECT
        concat_ws(
            '-',
            tx_hash,
            event_index
        ) AS log_id,
        block_id,
        block_timestamp,
        tx_hash,
        ingested_at,
        event_index,
        contract_address,
        contract_name,
        event_name,
        event_inputs,
        topics,
        DATA,
        event_removed
    FROM
        logs
)
SELECT
    log_id,
    block_id,
    block_timestamp,
    tx_hash,
    ingested_at,
    event_index,
    contract_address,
    contract_name,
    event_name,
    event_inputs,
    topics,
    DATA,
    event_removed
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY log_id
ORDER BY
    ingested_at DESC)) = 1
