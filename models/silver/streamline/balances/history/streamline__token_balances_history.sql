{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_token_balances(object_construct('node_name','flipsidenode', 'sql_source', '{{this.identifier}}'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 3
) {% for item in range(20) %}
    (
        SELECT
            block_number,
            address,
            contract_address
        FROM
            {{ ref("streamline__token_balances") }}
        WHERE
            block_number BETWEEN {{ item * 1000000 + 1 }}
            AND {{(
                item + 1
            ) * 1000000 }}
            AND block_number < (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
        EXCEPT
        SELECT
            block_number,
            address,
            contract_address
        FROM
            {{ ref("streamline__complete_token_balances") }}
        WHERE
            block_number BETWEEN {{ item * 1000000 + 1 }}
            AND {{(
                item + 1
            ) * 1000000 }}
            AND block_number < (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
        ORDER BY
            block_number
    ) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
