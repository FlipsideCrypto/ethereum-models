{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(contract_address)",
    tags = ['non_realtime']
) }}

WITH emitted_events AS (

    SELECT
        contract_address,
        COUNT(*) AS event_count,
        MAX(modified_timestamp) AS max_inserted_timestamp_logs,
        MAX(block_number) AS latest_event_block
    FROM
        {{ ref('core__fact_event_logs') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(max_inserted_timestamp_logs)
        FROM
            {{ this }}
    )
{% endif %}
GROUP BY
    contract_address
),
function_calls AS (
    SELECT
        IFF(
            TYPE = 'DELEGATECALL',
            from_address,
            to_address
        ) AS contract_address,
        COUNT(*) AS function_call_count,
        MAX(modified_timestamp) AS max_inserted_timestamp_traces,
        MAX(block_number) AS latest_call_block
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        tx_succeeded
        AND trace_succeeded
        AND to_address IS NOT NULL
        AND input IS NOT NULL
        AND input <> '0x'

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(max_inserted_timestamp_traces)
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    1
),
active_contracts AS (
    SELECT
        contract_address
    FROM
        emitted_events
    UNION
    SELECT
        contract_address
    FROM
        function_calls
),
previous_totals AS (

{% if is_incremental() %}
SELECT
    contract_address, total_event_count, total_call_count, max_inserted_timestamp_logs, latest_event_block, max_inserted_timestamp_traces, latest_call_block
FROM
    {{ this }}
{% else %}
SELECT
    NULL AS contract_address, 0 AS total_event_count, 0 AS total_call_count, '1970-01-01 00:00:00' AS max_inserted_timestamp_logs, 0 AS latest_event_block, '1970-01-01 00:00:00' AS max_inserted_timestamp_traces, 0 AS latest_call_block
{% endif %})
SELECT
    C.contract_address,
    COALESCE(
        p.total_event_count,
        0
    ) + COALESCE(
        e.event_count,
        0
    ) AS total_event_count,
    COALESCE(
        p.total_call_count,
        0
    ) + COALESCE(
        f.function_call_count,
        0
    ) AS total_call_count,
    COALESCE(
        p.total_event_count,
        0
    ) + COALESCE(
        e.event_count,
        0
    ) + COALESCE(
        p.total_call_count,
        0
    ) + COALESCE(
        f.function_call_count,
        0
    ) AS total_interaction_count,
    COALESCE(
        e.max_inserted_timestamp_logs,
        p.max_inserted_timestamp_logs,
        '1970-01-01 00:00:00'
    ) AS max_inserted_timestamp_logs,
    COALESCE(
        f.max_inserted_timestamp_traces,
        p.max_inserted_timestamp_traces,
        '1970-01-01 00:00:00'
    ) AS max_inserted_timestamp_traces,
    COALESCE(
        e.latest_event_block,
        p.latest_event_block,
        0
    ) AS latest_event_block,
    COALESCE(
        f.latest_call_block,
        p.latest_call_block,
        0
    ) AS latest_call_block
FROM
    active_contracts C
    LEFT JOIN emitted_events e
    ON C.contract_address = e.contract_address
    LEFT JOIN function_calls f
    ON C.contract_address = f.contract_address
    LEFT JOIN previous_totals p
    ON C.contract_address = p.contract_address
