{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_transactions(object_construct('sql_source', '{{this.identifier}}'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH last_3_days AS ({% if var('STREAMLINE_RUN_HISTORY') %}

    SELECT
        0 AS block_number
    {% else %}
    SELECT
        MAX(block_number) - 20000 AS block_number --aprox 3 days
    FROM
        {{ ref("streamline__blocks") }}
    {% endif %})
SELECT
    {{ dbt_utils.surrogate_key(
        ['block_number']
    ) }} AS id,
    block_number
FROM
    {{ ref("streamline__blocks") }}
WHERE
    (
        block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
    )
    AND block_number IS NOT NULL
EXCEPT
SELECT
    id,
    block_number
FROM
    {{ ref("streamline__complete_transactions") }}
WHERE
    block_number >= (
        SELECT
            block_number
        FROM
            last_3_days
    )
