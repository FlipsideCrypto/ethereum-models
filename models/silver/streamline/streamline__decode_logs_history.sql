{{ config (
    materialized = "view"
) }}

{% for item in range(15) %}
    (

        SELECT
            l.block_number,
            l._log_id,
            abi.data AS abi,
            l.data
        FROM
            {{ ref("streamline__decode_logs") }}
            l
            INNER JOIN {{ ref("silver__abis") }} abi
            ON l.abi_address = abi.contract_address
        WHERE
            l.block_number BETWEEN {{ item * 1000000 + 1 }}
            AND {{(
                item + 1
            ) * 1000000 }}
            AND _log_id NOT IN (
                SELECT
                    _log_id
                FROM
                    {{ ref("streamline__complete_decode_logs") }}
                WHERE
                    block_number BETWEEN {{ item * 1000000 + 1 }}
                    AND {{(
                        item + 1
                    ) * 1000000 }}
            )
        ORDER BY
            l.block_number
    ) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
