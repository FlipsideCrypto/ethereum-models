{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'confirm_blocks', 'method', 'eth_getBlockByNumber', 'producer_batch_size',5000, 'producer_limit_size', 5000000, 'worker_batch_size',500))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH look_back AS (

    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_hour") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 6
),
tbl AS (
    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        block_number IS NOT NULL
        AND block_number <= (
            SELECT
                block_number
            FROM
                look_back
        )
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_confirmed_blocks") }}
    WHERE
        block_number IS NOT NULL
        AND block_number <= (
            SELECT
                block_number
            FROM
                look_back
        )
)
SELECT
    block_number,
    'eth_getBlockByNumber' AS method,
    CONCAT(
        REPLACE(
            concat_ws('', '0x', to_char(block_number, 'XXXXXXXX')),
            ' ',
            ''
        ),
        '_-_',
        'false'
    ) AS params
FROM
    tbl
ORDER BY
    block_number ASC
