{{ config (
    materialized = "view",
) }}

{% for item in range(15) %}
    (

        SELECT
            block_number,
            address
        FROM
            {{ ref("streamline__eth_balances") }}
        WHERE
            block_number BETWEEN {{ item * 1000000 + 1 }}
            AND {{(
                item + 1
            ) * 1000000 }}
        EXCEPT
        SELECT
            block_number,
            address
        FROM
            {{ ref("streamline__complete_eth_balances") }}
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
