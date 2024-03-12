{% test missing_seaport_1_5(
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
        block_timestamp :: DATE >= '2023-04-25'
        AND contract_address = '0x00000000000000adc04c56bf30ac9d3c0aaf14dc'
        AND event_name IN (
            'OrderFulfilled',
            'OrdersMatched'
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
