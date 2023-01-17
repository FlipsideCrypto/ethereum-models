{{ config (
    materialized = "view",
    post_hook = if_data_call_function( 
        func = "{{this.schema}}.udf_generic_reads(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'beacon_committees', 'read_function', 'sync_committees', 'producer_batch_size', 20000000,'producer_limit_size', 20000000, 'producer_batch_chunks_size', 20))", 
        target = "{{this.schema}}.{{this.identifier}}" )
) }}

SELECT
    ROW_NUMBER() over (
        ORDER BY
            slot_number,
            state_id
    ) AS row_index,
    slot_number,
    state_id
FROM(
        SELECT
            slot_number,
            state_id
        FROM
            {{ ref("streamline__eth_committees") }}
    )
LIMIT 100
