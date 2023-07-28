{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_beacon_blocks(object_construct('sql_source', '{{this.identifier}}'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_beacon_history']
) }}

{% for item in range(5) %}
    (

        SELECT
            {{ dbt_utils.generate_surrogate_key(
                ['slot_number']
            ) }} AS id,
            slot_number
        FROM
            {{ ref("streamline__beacon_blocks") }}
        WHERE
            slot_number BETWEEN {{ item * 1000000 + 1 }}
            AND {{(
                item + 1
            ) * 1000000 }}
        EXCEPT
        SELECT
            id,
            slot_number
        FROM
            {{ ref("streamline__complete_beacon_blocks") }}
        WHERE
            slot_number BETWEEN {{ item * 1000000 + 1 }}
            AND {{(
                item + 1
            ) * 1000000 }}
        ORDER BY
            slot_number
    ) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
