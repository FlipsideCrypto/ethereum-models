{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_rest_api(object_construct('node_name','quicknode', 'sql_source','{{this.identifier}}', 'external_table','beacon_validators', 'route','validators', 'producer_batch_size', 10,'producer_limit_size', 100000, 'worker_batch_size', 1, 'producer_batch_chunks_size', 1))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH to_do AS (

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
)
SELECT
    slot_number,
    state_id
FROM
    to_do
UNION
SELECT
    slot_number,
    state_id
FROM
    {{ ref("_missing_validators") }}