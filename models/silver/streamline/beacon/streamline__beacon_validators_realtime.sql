{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_rest_api(object_construct('sql_source','{{this.identifier}}', 'external_table','beacon_validators', 'route','validators', 'producer_batch_size', 200,'producer_limit_size', 100000, 'worker_batch_size', 20, 'producer_batch_chunks_size', 2))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

SELECT
    block_number AS slot_number,
    state_id
FROM
    {{ ref("_max_beacon_block_by_date") }}
EXCEPT
SELECT
    slot_number,
    state_id
FROM
    {{ ref("streamline__complete_beacon_validators") }}
