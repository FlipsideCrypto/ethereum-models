{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_committees(object_construct('sql_source', '{{this.identifier}}'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

SELECT
    func_type,
    slot_number,
    state_id
FROM
    {{ ref("streamline__eth_committees") }}
EXCEPT
SELECT
    func_type,
    block_number AS slot_number,
    state_id
FROM
    {{ ref("streamline__complete_committees") }}

