{{ config (
    materialized = "ephemeral"
) }}

WITH lookback AS (

    SELECT
        MAX(block_number) AS block_lookback
    FROM
        {{ ref("silver__blocks") }}
    WHERE
        block_timestamp :: DATE = CURRENT_DATE() - 3
),
txs AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        block_hash
    FROM
        {{ ref("silver__transactions") }}
    WHERE
        block_number >= (
            SELECT
                block_lookback
            FROM
                lookback
        )
),
receipts AS (
    SELECT
        block_number,
        tx_hash,
        block_hash
    FROM
        {{ ref("silver__receipts") }}
    WHERE
        block_number >= (
            SELECT
                block_lookback
            FROM
                lookback
        )
)
SELECT
    DISTINCT COALESCE(
        t.block_number,
        r.block_number
    ) AS block_number
FROM
    txs t full
    OUTER JOIN receipts r
    ON t.block_number = r.block_number
    AND t.block_hash = r.block_hash
    AND t.tx_hash = r.tx_hash
WHERE
    r.tx_hash IS NULL
    OR t.tx_hash IS NULL
