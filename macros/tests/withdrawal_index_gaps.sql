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
        AND withdrawal_index + 1 <> next_index
),
series AS (
    SELECT
        ROW_NUMBER() over (
            ORDER BY
                NULL
        ) AS seq
    FROM
        TABLE(GENERATOR(rowcount => 1000))
),
FINAL AS (
    SELECT
        withdrawal_index,
        next_index,
        expected_index,
        start_slot_number,
        end_slot_number,
        start_slot_number + seq AS missing_slot_number
    FROM
        gaps
        JOIN series
        ON seq BETWEEN 1
        AND (
            end_slot_number - start_slot_number - 1
        )
    WHERE
        start_slot_number + seq < end_slot_number
)
SELECT
    DISTINCT missing_slot_number AS slot_number
FROM
    FINAL
{% endtest %}
