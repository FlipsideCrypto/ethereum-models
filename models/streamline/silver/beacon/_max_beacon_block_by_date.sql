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
    WHERE
        block_included
        AND TIME(slot_timestamp) = '23:59:59.000'
    GROUP BY
        slot_timestamp :: DATE
)
SELECT
    slot_timestamp :: DATE AS block_date,
    slot_number AS block_number,
    state_root AS state_id
FROM
    {{ ref("silver__beacon_blocks") }} A
    JOIN base b
    ON A.slot_number = b.block_number
WHERE
    block_date IS NOT NULL
