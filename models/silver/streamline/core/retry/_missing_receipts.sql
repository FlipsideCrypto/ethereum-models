{{ config (
    materialized = "ephemeral"
) }}

SELECT
    DISTINCT tx.block_number AS block_number
FROM
    {{ ref("silver__transactions") }}
    tx
    LEFT JOIN {{ ref("silver__receipts") }}
    r
    ON tx.block_number = r.block_number
    AND tx.tx_hash = r.tx_hash
WHERE
    tx.block_timestamp >= DATEADD(
        'day',
        -2,
        CURRENT_DATE
    )
    AND r.tx_hash IS NULL
