{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_reads(object_construct('node_name','flipsidenode'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_reads_history']
) }}

{% for item in range(17) %}
    (

        SELECT
            contract_address,
            function_signature,
            call_name,
            function_input,
            block_number
        FROM
            {{ ref("streamline__contract_reads") }}
        WHERE
            block_number BETWEEN {{ item * 1000000 + 1 }}
            AND {{(
                item + 1
            ) * 1000000 }}
        EXCEPT
        SELECT
            contract_address,
            function_signature,
            call_name,
            function_input,
            block_number
        FROM
            {{ ref("streamline__complete_reads") }}
        WHERE
            block_number BETWEEN {{ item * 1000000 + 1 }}
            AND {{(
                item + 1
            ) * 1000000 }}
        ORDER BY
            block_number
    ) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
