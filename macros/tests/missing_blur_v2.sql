{% test missing_blur_v2(
    model
) %}
WITH model_txs AS (
    SELECT
        DISTINCT tx_hash
    FROM
        {{ model }}
    WHERE
        block_timestamp <= CURRENT_DATE - 1
),
logs_txs AS (
    SELECT
        block_timestamp,
        tx_hash
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_timestamp :: DATE >= '2023-07-01'
        AND contract_address = '0xb2ecfe4e4d61f8790bbb9de2d1259b9e2410cea5'
        AND event_name IN (
            'Execution721MakerFeePacked',
            'Execution721TakerFeePacked',
            'Execution721Packed'
        )
        AND block_timestamp <= CURRENT_DATE - 1 qualify ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) = 1
)
SELECT
    block_timestamp :: DATE AS DAY,
    SUM(IFF(m.tx_hash IS NULL, 1, 0)) AS missing_tx,
    COUNT(
        DISTINCT l.tx_hash
    ) AS total_txs,
    missing_tx / total_txs * 100 AS missing_percentage
FROM
    logs_txs l
    LEFT JOIN model_txs m USING (tx_hash)
GROUP BY
    DAY
HAVING
    missing_percentage > 0.5 {% endtest %}
