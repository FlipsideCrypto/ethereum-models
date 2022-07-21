{% macro tx_gaps(
        model
    ) %}
    WITH block_base AS (
        SELECT
            block_number,
            tx_count
        FROM
            {{ ref('silver__blocks') }}
    ),
    model_name AS (
        SELECT
            block_number,
            COUNT(
                DISTINCT tx_hash
            ) AS model_tx_count
        FROM
            {{ model }}
        GROUP BY
            block_number
    )
SELECT
    block_base.block_number,
    tx_count,
    model_name.block_number AS model_block_number,
    model_tx_count
FROM
    block_base
    LEFT JOIN model_name
    ON block_base.block_number = model_name.block_number
WHERE
    tx_count <> model_tx_count
{% endmacro %}
