{{ config (
    materialized = "view",
) }}

{% for item in range(15) %}
    (

        SELECT
            block_number,
            _log_id,
            abi,
            DATA
        FROM
            {{ ref("streamline__decode_logs") }}
        WHERE
            block_number BETWEEN {{ item * 1000000 + 1 }}
            AND {{(
                item + 1
            ) * 1000000 }}
        EXCEPT
        SELECT
            block_number,
            _log_id,
            abi,
            DATA
        FROM
            {{ ref("streamline__complete_decoded_logs") }}
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
