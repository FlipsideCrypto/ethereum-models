{{ config (
    materialized = "view"
) }}

{% for item in range(10) %}
    (

        SELECT
            slot_number,
            func_type,
            state_id
        FROM
            {{ ref("streamline__eth_validators") }}
        WHERE
            slot_number BETWEEN {{ item * 1000000 + 1 }}
            AND {{(
                item + 1
            ) * 1000000 }}
        EXCEPT
        SELECT
            block_number AS slot_number,
            func_type,
            state_id
        FROM
            {{ ref("streamline__complete_validators") }}
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
