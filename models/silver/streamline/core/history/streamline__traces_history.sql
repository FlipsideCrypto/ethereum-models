{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'traces', 'route', 'trace_block', 'producer_batch_size',100, 'producer_limit_size', 500000, 'worker_batch_size',5, 'producer_batch_chunks_size', 20))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

{% for item in range(
        16,
        17
    ) %}
    (

        SELECT
            block_number,
            'trace_block' AS method,
            block_number_hex AS params
        FROM
            {{ ref("streamline__blocks") }}
        WHERE
            block_number BETWEEN {{ item * 1000000 + 1 }}
            AND {{(
                item + 1
            ) * 1000000 }}
        EXCEPT
        SELECT
            block_number,
            'trace_block' AS method,
            REPLACE(
                concat_ws('', '0x', to_char(block_number, 'XXXXXXXX')),
                ' ',
                ''
            ) AS params
        FROM
            {{ ref("streamline__complete_traces") }}
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