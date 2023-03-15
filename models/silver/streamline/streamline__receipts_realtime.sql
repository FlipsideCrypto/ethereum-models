{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'receipts', 'route', 'eth_getBlockReceipts', 'producer_batch_size', 100000, 'producer_limit_size', 20000000, 'worker_batch_size', 1000, 'producer_batch_chunks_size', 10000))",
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
    'eth_getBlockReceipts' AS method,
    block_number_hex AS params
FROM
    {{ ref("streamline__receipts") }}
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
    'eth_getBlockReceipts' AS method,
    REPLACE(
        concat_ws('', '0x', to_char(block_number, 'XXXXXXXX')),
        ' ',
        ''
    ) AS params
FROM
    {{ ref("streamline__complete_receipts") }}
WHERE
    block_number >= (
        SELECT
            block_number
        FROM
            last_3_days
    )
LIMIT 10000