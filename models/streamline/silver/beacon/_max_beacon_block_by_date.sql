{{ config (
    materialized = "ephemeral",
    unique_key = "block_number",
) }}

WITH base AS (

    SELECT
        slot_timestamp :: DATE AS block_date,
        MAX(slot_number) block_number
    FROM
        {{ ref("beacon_chain__fact_blocks") }}
    WHERE
        block_included
        AND TIME(slot_timestamp) = '23:59:59.000'
    GROUP BY
        slot_timestamp :: DATE
)
SELECT
    a.block_date,
    a.block_number,
    b.state_root AS state_id
FROM
    base a
    LEFT JOIN {{ ref("silver__beacon_blocks") }} b
    ON a.block_number = b.slot_number
WHERE
    block_date IS NOT NULL
