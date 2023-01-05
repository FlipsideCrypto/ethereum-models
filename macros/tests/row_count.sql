{% test row_count(
    model,
    group_field,
    count_field,
    threshold
) %}
SELECT
    row_count
FROM
    (
        SELECT
            group_field,
            COUNT(
                {{ count_field }}
            ) AS row_count
        FROM
            {{ model }}
        GROUP BY
            1
    )
WHERE
    row_count <= {{ threshold }}

    {% endtest %}
