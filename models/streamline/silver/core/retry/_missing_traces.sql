{{ config (
    materialized = "ephemeral"
) }}

SELECT
    DISTINCT tx.block_number
FROM
    {{ ref("test_silver__transactions_recent") }}
    tx
    LEFT JOIN {{ ref("test_gold__fact_traces_recent") }}
    tr USING (
        block_number,
        tx_hash
    )
WHERE
    tr.tx_hash IS NULL
    AND tx.block_timestamp > DATEADD('day', -5, SYSDATE())
