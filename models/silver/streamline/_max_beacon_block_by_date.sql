{{ config (
    materialized = "ephemeral",
    unique_key = "block_number",
) }}

WITH base AS (

    SELECT
        slot_timestamp :: DATE AS block_date,
        MAX(slot_number) block_number
    FROM
        {{ ref("silver__beacon_blocks") }}
    GROUP BY
        slot_timestamp :: DATE
)
SELECT
    block_date,
    block_number
FROM
    base
WHERE
    block_date <> (
        SELECT
            MAX(block_date)
        FROM
            base
    )
