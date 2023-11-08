{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_rest_api(object_construct('node_name','quicknode', 'sql_source','{{this.identifier}}', 'external_table','beacon_validators', 'route','validators', 'producer_batch_size', 10,'producer_limit_size', 100, 'worker_batch_size', 1, 'producer_batch_chunks_size', 1))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_beacon_history']
) }}

WITH to_do AS (

    SELECT
        slot_number,
        state_id
    FROM
        {{ ref("_premerge_max_daily_slots") }}
    EXCEPT
    SELECT
        slot_number,
        state_id
    FROM
        {{ ref("streamline__complete_beacon_validators") }}
)
SELECT
    slot_number,
    state_id
FROM
    to_do
WHERE
    state_id IS NOT NULL
