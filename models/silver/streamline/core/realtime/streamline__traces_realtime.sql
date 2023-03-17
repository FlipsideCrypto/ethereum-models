{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'traces', 'route', 'trace_block', 'producer_batch_size',100, 'producer_limit_size', 100000, 'worker_batch_size',10, 'producer_batch_chunks_size', 100))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH last_3_days AS ({% if var('STREAMLINE_RUN_HISTORY') %}

    SELECT
        0 AS block_number
    {% else %}
    SELECT
        MAX(block_number) - 20000 AS block_number --aprox 3 days
    FROM
        {{ ref("streamline__blocks") }}
    {% endif %})
SELECT
    block_number,
    'trace_block' AS method,
    block_number_hex AS params
FROM
    {{ ref("streamline__blocks") }}
WHERE
    (
        block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
    )
    AND block_number IS NOT NULL
EXCEPT
SELECT
    block_number,
    'trace_block' AS method,
    REPLACE(
        concat_ws('', '0x', to_char(block_number, 'XXXXXXXX')),
        ' ',
        ''
    ) AS params
FROM
    {{ ref("streamline__complete_traces") }}
WHERE
    block_number >= (
        SELECT
            block_number
        FROM
            last_3_days
    )
