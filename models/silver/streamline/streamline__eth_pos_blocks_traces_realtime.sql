{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_generic_jsonrpc_reads(object_construct('nested', 'false', 'sql_source', '{{this.identifier}}', 'external_table', 'beacon_blocks_traces', 'route', 'debug_traceBlockByNumber', 'producer_batch_size', 10000, 'producer_limit_size', 20000000, 'worker_batch_size', 100, 'producer_batch_chunks_size', 100))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH last_3_days AS (

    SELECT
        block_number AS slot_number
    FROM
        {{ ref("_max_beacon_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 3
)
SELECT
    slot_number AS block_number,
    'debug_traceBlockByNumber' AS method,
    slot_number_hex AS params
FROM
    {{ ref("streamline__eth_pos_blocks_receipts") }}
WHERE
    (
        slot_number < (
            SELECT
                slot_number
            FROM
                last_3_days
        )
    )
    AND slot_number IS NOT NULL
LIMIT 10000
