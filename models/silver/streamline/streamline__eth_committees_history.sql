{{ config (
    materialized = "view"
) }}

{% for item in range(100000) %}
    (

        SELECT
            slot_number,
            func_type,
            state_id
        FROM
            {{ ref("streamline__eth_committees") }}
        WHERE
            slot_number BETWEEN {{ item * 100 + 1 }}
            AND {{(
                item + 1
            ) * 100 }}
        EXCEPT
        SELECT
            block_number AS slot_number,
            func_type,
            state_id
        FROM
            {{ ref("streamline__complete_committees") }}
        WHERE
            slot_number BETWEEN {{ item * 100 + 1 }}
            AND {{(
                item + 1
            ) * 100 }}
        ORDER BY
            slot_number
    ) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
