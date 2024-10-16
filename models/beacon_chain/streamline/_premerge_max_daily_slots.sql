{{ config (
    materialized = "ephemeral"
) }}

WITH slots AS (

    SELECT
        FLOOR((_id - 3599) / 7200) + 1 AS day_number,
        MAX(_id) AS max_slot
    FROM
        {{ ref("silver__number_sequence") }}
    WHERE
        _id BETWEEN 3598
        AND 4700012
    GROUP BY
        day_number
),
adjusted_slots AS (
    --these slots are skipped
    SELECT
        CASE
            WHEN max_slot IN (
                961198,
                4337998,
                4402798
            ) THEN max_slot - 1
            ELSE max_slot
        END AS max_slot
    FROM
        slots
)
SELECT
    slot_number,
    state_root AS state_id
FROM
    {{ ref("silver__beacon_blocks") }}
    JOIN adjusted_slots
    ON slot_number = max_slot
