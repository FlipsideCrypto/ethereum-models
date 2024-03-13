{% test missing_blur_v2_logs(model) %}
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
        {{ ref('silver__logs') }}
    WHERE
        block_timestamp <= CURRENT_DATE - 1
        AND block_timestamp :: DATE >= '2023-07-01'
        AND contract_address = '0xb2ecfe4e4d61f8790bbb9de2d1259b9e2410cea5'
        AND topics[0]::string in (
           '0x0fcf17fac114131b10f37b183c6a60f905911e52802caeeb3e6ea210398b81ab',
           '0x7dc5c0699ac8dd5250cbe368a2fc3b4a2daadb120ad07f6cccea29f83482686e',
           '0x1d5e12b51dee5e4d34434576c3fb99714a85f57b0fd546ada4b0bddd736d12b2'
        )
        qualify ROW_NUMBER() over (
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
    {% test missing_seaport_1_5_logs(model) %}
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
            {{ ref('silver__logs') }}
        WHERE
            block_timestamp <= CURRENT_DATE - 1
            AND block_timestamp :: DATE >= '2023-04-25'
            AND contract_address = '0x00000000000000adc04c56bf30ac9d3c0aaf14dc'
            AND topics[0]::string in (
                '0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31',
                '0x4b9f2d36e1b4c93de62cc077b00b1a91d84b6c31b4a14e012718dcca230689e7'
                )
            qualify ROW_NUMBER() over (
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
    {% test missing_magiceden_logs(model) %}
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
            {{ ref('silver__logs') }}
        WHERE
            block_timestamp <= CURRENT_DATE - 1
            AND block_timestamp :: DATE >= '2024-02-04'
            AND contract_address = '0x9a1d00bed7cd04bcda516d721a596eb22aac6834'
            AND origin_to_address = '0x5ebc127fae83ed5bdd91fc6a5f5767e259df5642'
            AND topics[0]::string in (
                '0x8b87c0b049fe52718fe6ff466b514c5a93c405fb0de8fbd761a23483f9f9e198',
                '0x1217006325a98bdcc6afc9c44965bb66ac7460a44dc57c2ac47622561d25c45a',
                '0xffb29e9cf48456d56b6d414855b66a7ec060ce2054dcb124a1876310e1b7355c',
                '0x6f4c56c4b9a9d2479f963d802b19d17b02293ce1225461ac0cb846c482ee3c3e'
                )
            qualify ROW_NUMBER() over (
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
