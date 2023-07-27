{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_json_rpc(object_construct('node_name','quicknode', 'sql_source', '{{this.identifier}}', 'external_table', 'receipts', 'exploded_key','[\"result\"]', 'route', 'eth_getBlockReceipts', 'producer_batch_size',100, 'producer_limit_size', 100000, 'worker_batch_size',10, 'producer_batch_chunks_size', 100))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_core_realtime']
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
),
to_do AS (
    SELECT
        block_number,
        'eth_getBlockReceipts' AS method,
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
    'eth_getBlockReceipts' AS method,
    REPLACE(
        concat_ws('', '0x', to_char(block_number, 'XXXXXXXX')),
        ' ',
        ''
    ) AS params
FROM
    (
        SELECT
            block_number
        FROM
            {{ ref("_missing_receipts") }}
        UNION
        SELECT
            block_number
        FROM
            {{ ref("_missing_txs") }}
        UNION
        SELECT
            block_number
        FROM
            {{ ref("_unconfirmed_blocks") }}
    )
