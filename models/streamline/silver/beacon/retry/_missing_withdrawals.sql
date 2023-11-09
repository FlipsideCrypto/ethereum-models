{{ config (
    materialized = "ephemeral"
) }}

WITH base AS (

    SELECT
        INDEX AS withdrawal_index,
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
        {{ ref('silver__beacon_withdrawals') }}
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
        _id AS seq
    FROM
        {{ ref('silver__number_sequence') }}
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
    DISTINCT missing_slot_number AS slot_number,
    {{ dbt_utils.generate_surrogate_key(
        ['slot_number']
    ) }} AS id
FROM
    FINAL
