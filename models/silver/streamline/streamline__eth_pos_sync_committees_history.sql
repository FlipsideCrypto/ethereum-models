{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_generic_reads(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'beacon_sync_committees', 'route', 'sync_committees', 'producer_batch_size', 100000,'producer_limit_size', 20000000, 'worker_batch_size', 5000, 'producer_batch_chunks_size', 200))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

SELECT
    slot_number,
    state_id
FROM
    {{ ref("streamline__eth_pos_sync_committees") }}
WHERE
    slot_number IS NOT NULL
EXCEPT
SELECT
    slot_number,
    state_id
FROM
    {{ ref("streamline__complete_eth_pos_sync_committees") }}
