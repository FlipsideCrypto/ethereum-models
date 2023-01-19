{% test avg_row_count(
    model,
    group_field,
    count_field,
    lookback_condition,
    threshold,
    above_flag
) %}
WITH base AS (
    SELECT
        {{ group_field }} AS group_field,
        COUNT(
            {{ count_field }}
        ) AS count_field
    FROM
        {{ model }}
    WHERE
        {{ lookback_condition }}
    GROUP BY
        1
),
trailing_avg AS (
    SELECT
        AVG(count_field) AS avg_rows
    FROM
        base
),
FINAL AS (
    SELECT
        *,
        ROUND(
            count_field / avg_rows,
            2
        ) AS diff
    FROM
        base
        JOIN trailing_avg
        ON 1 = 1
)
SELECT
    *
FROM
    FINAL
WHERE
    CASE
        WHEN {{ above_flag }} THEN diff NOT BETWEEN (
            1 - {{ threshold }}
        )
        AND (
            {{ threshold }} + 1
        )
        ELSE diff < 1 - {{ threshold }}
    END {% endtest %}
