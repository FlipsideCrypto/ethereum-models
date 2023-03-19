{{ config(
    materialized = 'incremental',
    unique_key = "tx_hash",
    incremental_strategy = 'delete+insert',
    cluster_by = "block_timestamp::date, _inserted_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH base_txs AS (

    SELECT
        block_number,
        blockHash,
        cumulativeGasUsed,
        effectiveGasPrice,
        from_address,
        gasUsed,
        logs,
        logsBloom,
        status,
        to_address,
        tx_hash,
        transactionIndex,
        TYPE,
        _inserted_timestamp
    FROM
        {{ ref('silver__streamline_receipts') }}

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
flat_data AS (
    SELECT
        block_number,
        tx_hash,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        status AS tx_status,
        _inserted_timestamp,
        PUBLIC.udf_hex_to_int(
            VALUE :logIndex :: STRING
        ) AS event_index,
        VALUE :address :: STRING AS contract_address,
        VALUE :topics AS topics,
        VALUE :data :: STRING AS DATA,
        VALUE :removed :: STRING AS event_removed
    FROM
        logs_raw,
        LATERAL FLATTEN (
            input => full_logs
        )
),
new_records AS (
    SELECT
        f.block_number,
        block_timestamp,
        f.tx_hash,
        f.origin_from_address,
        f.origin_to_address,
        f.tx_status,
        f._inserted_timestamp,
        f.event_index,
        f.contract_address,
        f.topics,
        f.data,
        f.event_removed,
        origin_function_signature,
        CASE
            WHEN origin_function_signature IS NULL
            OR block_timestamp IS NULL THEN TRUE
            ELSE FALSE
        END AS is_pending,
        conat(
            f.tx_hash,
            '-',
            f.event_index
        ) AS _log_id
    FROM
        flat_data f
        LEFT OUTER JOIN {{ ref('bronze__streamline_transactions') }}
        t
        ON f.block_number = t.block_number
        AND f.tx_hash = t.tx_hash
        LEFT OUTER JOIN {{ ref('bronze__streamline_blocks') }}
        b
        ON f.block_number = b.block_number
)

{% if is_incremental() %},
missing_data AS (
    SELECT
        t.block_number,
        b.block_timestamp,
        t.tx_hash,
        t.origin_from_address,
        t.origin_to_address,
        t.tx_status,
        GREATEST(
            t._inserted_timestamp,
            b._inserted_timestamp,
            txs._inserted_timestamp
        ) AS _inserted_timestamp,
        t.event_index,
        t.contract_address,
        t.topics,
        t.data,
        t.event_removed,
        txs.origin_function_signature,
        FALSE AS is_pending,
        t._log_id
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('bronze__streamline_blocks') }}
        b
        ON t.block_number = b.block_number
        INNER JOIN {{ ref('bronze__streamline_transactions') }}
        txs
        ON txs.tx_hash = t.tx_hash
        AND t.block_number = txs.block_number
    WHERE
        t.is_pending
)
{% endif %}
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_from_address,
    origin_to_address,
    tx_status,
    _inserted_timestamp,
    event_index,
    contract_address,
    topics,
    DATA,
    event_removed,
    origin_function_signature,
    is_pending,
    _log_id
FROM
    new_records qualify(ROW_NUMBER() over (PARTITION BY block_number, _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1

{% if is_incremental() %}
UNION
SELECT
    *
FROM
    missing_data
{% endif %}
