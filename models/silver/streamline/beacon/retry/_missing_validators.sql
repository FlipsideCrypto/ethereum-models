{{ config (
    materialized = "ephemeral"
) }}

WITH validators AS (

    SELECT
        COUNT(
            DISTINCT INDEX
        ) AS num,
        block_number,
        state_id
    FROM
        {{ ref('silver__beacon_validators') }}
    WHERE
        _inserted_timestamp >= DATEADD(
            'day',
            -2,
            CURRENT_DATE
        )
    GROUP BY
        2,
        3
),
ordered_data AS (
    SELECT
        num,
        block_number,
        state_id,
        LAG(num) over (
            ORDER BY
                block_number
        ) AS prev_num
    FROM
        validators
)
SELECT
    DISTINCT block_number AS slot_number,
    state_id
FROM
    ordered_data
WHERE
    num < prev_num
