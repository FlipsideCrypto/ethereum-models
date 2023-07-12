{{ config (
    materialized = "ephemeral"
) }}

SELECT
    DISTINCT tx.block_number block_number
FROM
    {{ ref("silver__transactions") }}
    tx
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
    tx.block_timestamp >= DATEADD(
        'day',
        -2,
        CURRENT_DATE
    )
    AND tr.tx_hash IS NULL
