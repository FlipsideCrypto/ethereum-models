{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_generic_jsonrpc_reads(object_construct('nested', 'false', 'sql_source', '{{this.identifier}}', 'external_table', 'beacon_txs_receipts', 'route', 'eth_getTransactionReceipt', 'producer_batch_size', 1000000, 'producer_limit_size', 20000000, 'worker_batch_size', 5000, 'producer_batch_chunks_size', 5000))",
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
    block_number,
    'eth_getTransactionReceipt' AS method,
    tx_id AS params
FROM
    {{ ref("streamline__eth_pos_txs_receipts") }}
WHERE
    (
        block_number < (
            SELECT
                slot_number
            FROM
                last_3_days
        )
    )
    AND block_number IS NOT NULL
