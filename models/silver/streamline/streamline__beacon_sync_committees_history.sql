{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_rest_api(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'beacon_sync_committees', 'route', 'sync_committees', 'producer_batch_size',50000, 'producer_limit_size',20000, 'worker_batch_size',2000, 'producer_batch_chunks_size',100))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

SELECT
    slot_number,
    state_id
FROM
    {{ ref("streamline__beacon_sync_committees") }}
WHERE
    slot_number IS NOT NULL
EXCEPT
SELECT
    slot_number,
    state_id
FROM
    {{ ref("streamline__complete_beacon_sync_committees") }}
