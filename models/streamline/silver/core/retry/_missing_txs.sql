{{ config (
    materialized = "ephemeral"
) }}

WITH lookback AS (

    SELECT
        block_number
    FROM
        {{ ref("_block_lookback") }}
),
transactions AS (
    SELECT
        block_number,
        POSITION,
        LAG(
            POSITION,
            1
        ) over (
            PARTITION BY block_number
            ORDER BY
                POSITION ASC
        ) AS prev_POSITION
    FROM
        {{ ref("silver__transactions") }}
    WHERE
        block_timestamp >= DATEADD('hour', -84, SYSDATE())
        AND block_number >= (
            SELECT
                block_number
            FROM
                lookback
        )
)
SELECT
    DISTINCT block_number AS block_number
FROM
    transactions
WHERE
    POSITION - prev_POSITION <> 1
