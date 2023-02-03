{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'traces', 'route', 'debug_traceBlockByNumber', 'producer_batch_size', 100, 'producer_limit_size', 20000000, 'worker_batch_size', 20, 'producer_batch_chunks_size', 5))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 3
)
SELECT
    block_number,
    'debug_traceBlockByNumber' AS method,
    block_number_hex AS params
FROM
    {{ ref("streamline__traces") }}
ORDER BY
    block_number DESC
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
    'debug_traceBlockByNumber' AS method,
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
