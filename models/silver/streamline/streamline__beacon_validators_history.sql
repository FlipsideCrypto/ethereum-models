{{ config (
    materialized = "view",
) }}

{% for item in range(15) %}
    (

        SELECT
            {{ dbt_utils.surrogate_key(
                ['block_number']
            ) }} AS id,
            state_root,
            block_number
        FROM
            {{ ref("streamline__beacon_validators") }}
        WHERE
            block_number BETWEEN {{ item * 1000000 + 1 }}
            AND {{(
                item + 1
            ) * 1000000 }}
        EXCEPT
        SELECT
            id,
            state_root,
            block_number
        FROM
            {{ ref("streamline__complete_beacon_validators") }}
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
