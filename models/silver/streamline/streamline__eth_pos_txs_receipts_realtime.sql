{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_generic_jsonrpc_reads(object_construct('nested', 'false', 'sql_source', '{{this.identifier}}', 'external_table', 'beacon_txs_receipts', 'route', 'eth_getTransactionReceipt', 'producer_batch_size', 200000, 'producer_limit_size', 20000000, 'worker_batch_size', 5000, 'producer_batch_chunks_size', 5000))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

SELECT
    block_number,
    'eth_getTransactionReceipt' AS method,
    tx_id AS params
FROM
    {{ ref("streamline__eth_pos_txs_receipts") }}
EXCEPT
SELECT
    slot_number AS block_number,
    'eth_getTransactionReceipt' AS method,
    tx_id AS params
FROM
    {{ ref("streamline__complete_eth_pos_txs_receipts") }}
ORDER BY block_number ASC
LIMIT 10000
