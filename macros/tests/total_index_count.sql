{% test total_index_count(
    model,
    column_name
) %}
WITH base AS (
    SELECT
        COUNT(
            DISTINCT {{ column_name }}
        ) AS num,
        block_number
    FROM
        {{ model }}
    GROUP BY
        2
),
ordered_data AS (
    SELECT
        num,
        block_number,
        LAG(num) over (
            ORDER BY
                block_number
        ) AS prev_num
    FROM
        base
)
SELECT
    block_number
FROM
    ordered_data
WHERE
    num < prev_num
{% endtest %}
