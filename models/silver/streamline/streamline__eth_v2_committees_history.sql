{{ config (
    materialized = "view",
) }}

{% for item in range(15) %}
    (

        SELECT
            {{ dbt_utils.surrogate_key(
                ['type', 'block_number', 'state_id']
            ) }} AS id,
            TYPE,
            block_number,
            state_id
        FROM
            {{ ref("streamline__eth_v2_committees") }}
        WHERE
            block_number BETWEEN {{ item * 1000000 + 1 }}
            AND {{(
                item + 1
            ) * 1000000 }}
        EXCEPT
        SELECT
            id,
            TYPE,
            block_number,
            state_id
        FROM
            {{ ref("streamline__complete_eth_v2_committees") }}
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
