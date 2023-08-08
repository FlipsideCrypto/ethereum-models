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
    DISTINCT cb.block_number AS block_number
FROM
    {{ ref("silver__confirmed_blocks") }}
    cb
    LEFT JOIN {{ ref("silver__transactions") }}
    txs USING (
        block_number,
        block_hash,
        tx_hash
    )
WHERE
    txs.tx_hash IS NULL
    AND cb.block_number >= (
        SELECT
            block_number
        FROM
            lookback
    )
