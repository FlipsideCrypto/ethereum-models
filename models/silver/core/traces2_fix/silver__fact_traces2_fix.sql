{# {{ config (
materialized = "incremental",
incremental_strategy = 'delete+insert',
unique_key = ["block_number", "tx_position", "trace_address"],
tags = ['traces_fix']
) }}

{% set batch_query %}
SELECT
    MAX(next_batch_id) AS next_batch_id
FROM
    (
        SELECT
            1 AS next_batch_id

{% if is_incremental() %}
UNION ALL
SELECT
    COALESCE(MAX(batch_id), 0) + 1 AS next_batch_id
FROM
    {{ this }}
{% endif %}) {% endset %}
{% if execute %}
    {% set result = run_query(batch_query) %}
    {{ log(
        "Debug - Batch Query result: " ~ result,
        info = True
    ) }}

    {% set batch_id = result.columns [0] [0] %}
    {% if batch_id > 43 %}
        {{ exceptions.raise_compiler_error("Processing complete - reached max batch_id of 43") }}
    {% endif %}

    {% set block_size = 500000 %}
    {% set block_start = 1 + (
        batch_id - 1
    ) * block_size %}
    {% set block_end = batch_id * block_size %}
    {{ log(
        "Processing batch_id: " ~ batch_id ~ ", blocks: " ~ block_start ~ " to " ~ block_end,
        info = True
    ) }}
{% endif %}

WITH silver_traces AS (
    SELECT
        block_number,
        tx_position,
        trace_address,
        parent_trace_address,
        trace_json
    FROM
        {{ ref('silver__traces2') }}
    WHERE
        block_number BETWEEN {{ block_start }}
        AND {{ block_end }}
),
errored_traces AS (
    SELECT
        block_number,
        tx_position,
        trace_address,
        trace_json
    FROM
        silver_traces
    WHERE
        trace_json :error :: STRING IS NOT NULL
),
error_logic AS (
    SELECT
        b0.block_number,
        b0.tx_position,
        b0.trace_address,
        b0.trace_json :error :: STRING AS error,
        b1.trace_json :error :: STRING AS any_error,
        b2.trace_json :error :: STRING AS origin_error
    FROM
        silver_traces b0
        LEFT JOIN errored_traces b1
        ON b0.block_number = b1.block_number
        AND b0.tx_position = b1.tx_position
        AND b0.trace_address RLIKE CONCAT(
            '^',
            b1.trace_address,
            '(_[0-9]+)*$'
        )
        LEFT JOIN errored_traces b2
        ON b0.block_number = b2.block_number
        AND b0.tx_position = b2.tx_position
        AND b2.trace_address = 'ORIGIN'
),
aggregated_errors AS (
    SELECT
        block_number,
        tx_position,
        trace_address,
        error,
        IFF(MAX(any_error) IS NULL
        AND error IS NULL
        AND origin_error IS NULL, TRUE, FALSE) AS trace_succeeded
    FROM
        error_logic
    GROUP BY
        block_number,
        tx_position,
        trace_address,
        error,
        origin_error),
        prod AS (
            SELECT
                block_number,
                tx_position,
                tx_hash,
                trace_address,
                trace_succeeded AS prod_trace_succeeded
            FROM
                {{ ref('silver__fact_traces2') }}
            WHERE
                block_number BETWEEN {{ block_start }}
                AND {{ block_end }}
        ),
        final_errors AS (
            SELECT
                block_number,
                tx_position,
                trace_address,
                error,
                trace_succeeded,
                prod_trace_succeeded
            FROM
                aggregated_errors
                INNER JOIN prod USING (
                    block_number,
                    tx_position,
                    trace_address
                )
            WHERE
                prod_trace_succeeded != trace_succeeded
            UNION ALL
            SELECT
                NULL AS block_number,
                NULL AS tx_position,
                NULL AS trace_address,
                NULL AS error,
                NULL AS trace_succeeded,
                NULL AS prod_trace_succeeded
        ),
        batch AS (
            SELECT
                CAST({{ batch_id }} AS NUMBER(3, 0)) AS batch_id
        )
    SELECT
        batch_id,
        block_number,
        tx_position,
        trace_address,
        error,
        trace_succeeded,
        prod_trace_succeeded
    FROM
        batch
        CROSS JOIN final_errors #}
