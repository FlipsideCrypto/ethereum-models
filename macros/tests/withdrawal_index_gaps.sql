{% test withdrawal_index_gaps(
    model,
    column_name
) %}
WITH base AS (
    SELECT
        {{ column_name }} AS withdrawal_index,
        LEAD(withdrawal_index) over (
            ORDER BY
                withdrawal_index
        ) AS next_index,
        slot_number AS start_slot_number,
        LEAD(slot_number) over (
            ORDER BY
                withdrawal_index
        ) AS end_slot_number
    FROM
        {{ model }}
),
gaps AS (
    SELECT
        withdrawal_index,
        next_index,
        withdrawal_index + 1 AS expected_index,
        start_slot_number,
        end_slot_number
    FROM
        base
    WHERE
        next_index IS NOT NULL
        AND expected_index <> next_index
)
SELECT
    next_index - withdrawal_index AS gaps
FROM
    gaps 
{% endtest %}
