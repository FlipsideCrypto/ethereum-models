{{ config (
    materialized = "ephemeral"
) }}

WITH lookback AS (

    SELECT
        MAX(block_number) AS block_number
    FROM
        {{ ref("silver__blocks") }}
    WHERE
        block_timestamp :: DATE = CURRENT_DATE() - 3
)
SELECT
    DISTINCT t.block_number AS block_number
FROM
    {{ ref("silver__transactions") }}
    t
    LEFT JOIN {{ ref("silver__receipts") }}
    r USING (
        block_number,
        block_hash,
        tx_hash
    )
WHERE
    r.tx_hash IS NULL
    AND t.block_number >= (
        SELECT
            block_number
        FROM
            lookback
    )
