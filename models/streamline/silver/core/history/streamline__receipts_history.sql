{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'receipts', 'exploded_key','[\"result\"]', 'route', 'eth_getBlockReceipts', 'producer_batch_size', 100000, 'producer_limit_size', 20000000, 'worker_batch_size', 1000, 'producer_batch_chunks_size', 10000))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_core_history']
) }}

{% for item in range(
        18
    ) %}
    (

        SELECT
            block_number,
            'eth_getBlockReceipts' AS method,
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
            'eth_getBlockReceipts' AS method,
            REPLACE(
                concat_ws('', '0x', to_char(block_number, 'XXXXXXXX')),
                ' ',
                ''
            ) AS params
        FROM
            {{ ref("streamline__complete_receipts") }}
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
