{% test dupe_index_days(
    model,
    column_name
) %}
WITH daily_index_counts AS (
    SELECT
        DATE_TRUNC(
            'day',
            b.slot_timestamp
        ) AS slot_date,
        v.{{ column_name }},
        COUNT(*) AS appearances_per_day
    FROM
        {{ model }}
        v
        LEFT JOIN ethereum_dev.silver.beacon_blocks b
        ON v.block_number = b.slot_number
    WHERE
        b.slot_timestamp :: DATE >= SYSDATE() - INTERVAL '60 day'
    GROUP BY
        1,
        2
    HAVING
        COUNT(*) > 1
)
SELECT
    slot_date,
    {{ column_name }},
    appearances_per_day
FROM
    daily_index_counts
ORDER BY
    slot_date,
    {{ column_name }}

    {% endtest %}
