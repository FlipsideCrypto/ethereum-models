-- {% set model_1 = 'silver_nft.blur_v2' %}
-- {% set model_2 = 'silver_nft.seaport_1_5_sales' %}
-- {% set model_3 = 'silver_nft.magiceden_sales' %}

{% test missing_nft_logs(model) %}

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

        WHERE block_timestamp <= CURRENT_DATE - 1

        {% if model == ref('silver_nft__blur_v2') %}
        AND
            block_timestamp :: DATE >= '2023-07-01'
            AND contract_address = '0xb2ecfe4e4d61f8790bbb9de2d1259b9e2410cea5'
            AND event_name IN (
                'Execution721MakerFeePacked',
                'Execution721TakerFeePacked',
                'Execution721Packed'
            )
        {% elif model == ref('silver_nft__seaport_1_5_sales') %}
        AND
            block_timestamp :: DATE >= '2023-04-25'
            AND contract_address = '0x00000000000000adc04c56bf30ac9d3c0aaf14dc'
            AND event_name IN (
                'OrderFulfilled',
                'OrdersMatched'
            ) 
        {% elif model == ref('silver_nft__magiceden_sales') %}
        AND
            block_timestamp :: DATE >= '2024-02-04'
            AND contract_address = '0x9a1d00bed7cd04bcda516d721a596eb22aac6834'
            AND origin_to_address = '0x5ebc127fae83ed5bdd91fc6a5f5767e259df5642'
            AND event_name IN (
                'BuyListingERC1155',
                'BuyListingERC721',
                'AcceptOfferERC1155',
                'AcceptOfferERC721'
            )
        {% endif %}
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
