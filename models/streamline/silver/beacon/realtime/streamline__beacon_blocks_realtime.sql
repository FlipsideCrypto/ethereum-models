{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_beacon_blocks(object_construct('sql_source', '{{this.identifier}}'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_beacon_realtime']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['slot_number']
    ) }} AS id,
    slot_number
FROM
    {{ ref("streamline__beacon_blocks") }}
WHERE
    slot_number > 5000000
    AND slot_number IS NOT NULL
EXCEPT
SELECT
    id,
    slot_number
FROM
    {{ ref("streamline__complete_beacon_blocks") }}
WHERE
    slot_number > 5000000
