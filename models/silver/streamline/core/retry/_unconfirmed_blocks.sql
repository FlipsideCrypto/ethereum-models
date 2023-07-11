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
confirmed_blocks AS (
    SELECT
        cb.block_number AS confirmed_block_number,
        cb.block_hash AS confirmed_block_hash,
        cb.tx_hash AS confirmed_tx_hash,
        txs.tx_hash AS tx_hash
    FROM
        {{ ref("silver__confirmed_blocks") }}
        cb full
        OUTER JOIN {{ ref("silver__transactions") }}
        txs
        ON cb.block_number = txs.block_number
        AND cb.block_hash = txs.block_hash
        AND cb.tx_hash = txs.tx_hash
    WHERE
        cb.block_number >= (
            SELECT
                block_lookback
            FROM
                lookback
        )
        AND txs.block_number >= (
            SELECT
                block_lookback
            FROM
                lookback
        )
        AND (
            txs.tx_hash IS NULL
            OR cb.tx_hash IS NULL
        )
        AND txs.block_number <= (
            SELECT
                MAX(block_number)
            FROM
                {{ ref("silver__confirmed_blocks") }}
        )
)
SELECT
    DISTINCT confirmed_block_number AS block_number
FROM
    confirmed_blocks
