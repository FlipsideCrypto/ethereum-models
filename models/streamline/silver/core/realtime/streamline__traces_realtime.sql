{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_json_rpc(object_construct('node_name','quicknode', 'sql_source', '{{this.identifier}}', 'external_table', 'traces', 'exploded_key','[\"result\"]', 'route', 'debug_traceBlockByNumber', 'producer_batch_size',100, 'producer_limit_size', 100000, 'worker_batch_size',10, 'producer_batch_chunks_size', 100))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_core_realtime']
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_block_lookback") }}
),
to_do AS (
    SELECT
        block_number,
        'debug_traceBlockByNumber' AS method,
        CONCAT(
            block_number_hex,
            '_-_',
            '{"tracer": "callTracer"}'
        ) AS params
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
        'debug_traceBlockByNumber' AS method,
        CONCAT(
            REPLACE(
                concat_ws('', '0x', to_char(block_number, 'XXXXXXXX')),
                ' ',
                ''
            ),
            '_-_',
            '{"tracer": "callTracer"}'
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
        AND _inserted_timestamp >= DATEADD(
            'day',
            -4,
            SYSDATE()
        )
)
SELECT
    block_number,
    method,
    params
FROM
    to_do
UNION
SELECT
    block_number,
    'debug_traceBlockByNumber' AS method,
    CONCAT(
        REPLACE(
            concat_ws('', '0x', to_char(block_number, 'XXXXXXXX')),
            ' ',
            ''
        ),
        '_-_',
        '{"tracer": "callTracer"}'
    ) AS params
FROM
    (
        SELECT
            block_number
        FROM
            {{ ref("_missing_traces") }}
        UNION
        SELECT
            block_number
        FROM
            {{ ref("_unconfirmed_blocks") }}
    )
ORDER BY
    block_number ASC
LIMIT
    300
