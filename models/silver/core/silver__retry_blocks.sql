{{ config (
    materialized = "view"
) }}

WITH transactions AS (

    SELECT
        block_number,
        tx_hash,
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
        block_timestamp >= DATEADD(
            'day',
            -2,
            CURRENT_DATE
        )
),
missing_txs AS (
    SELECT
        DISTINCT block_number
    FROM
        transactions
    WHERE
        POSITION - prev_POSITION <> 1
),
missing_traces AS (
    SELECT
        DISTINCT tx.block_number AS block_number
    FROM
        transactions tx
        LEFT JOIN {{ ref("silver__traces") }}
        tr
        ON tx.block_number = tr.block_number
        AND tx.tx_hash = tr.tx_hash
        AND tr.block_timestamp >= DATEADD(
            'day',
            -2,
            CURRENT_DATE
        )
    WHERE
        (
            tr.tx_hash IS NULL
            OR tr.block_number IS NULL
        )
)
SELECT
    block_number
FROM
    missing_txs
UNION
SELECT
    block_number
FROM
    missing_traces
