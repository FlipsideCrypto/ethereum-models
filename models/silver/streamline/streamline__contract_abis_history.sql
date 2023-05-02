{{ config (
    materialized = "view",
) }}

{% for item in range(15) %}
    (

        SELECT
            {{ dbt_utils.generate_surrogate_key(
                ['contract_address', 'block_number']
            ) }} AS id,
            contract_address,
            block_number
        FROM
            {{ ref("streamline__contract_addresses") }}
        WHERE
            block_number BETWEEN {{ item * 1000000 + 1 }}
            AND {{(
                item + 1
            ) * 1000000 }}
        EXCEPT
        SELECT
            id,
            contract_address,
            block_number
        FROM
            {{ ref("streamline__complete_contract_abis") }}
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
