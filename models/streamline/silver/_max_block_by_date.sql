{{ config (
    materialized = "ephemeral",
    unique_key = "block_number",
) }}

WITH base AS (

    SELECT
        block_timestamp :: DATE AS block_date,
        MAX(block_number) block_number
    FROM
        {{ ref("silver__blocks") }}
    GROUP BY
        block_timestamp :: DATE
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