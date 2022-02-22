{{ config(
    materialized = 'incremental',
    unique_key = "tx_hash",
    incremental_strategy = 'delete+insert',
    cluster_by = ['ingested_at::DATE'],
    tags = ['snowflake', 'ethereum', 'silver_ethereum', 'ethereum_transactions']
) }}

WITH base_table AS (

    SELECT
        block_timestamp,
        tx :block_number AS block_number,
        tx_id :: STRING AS tx_hash,
        silver_ethereum_2022.js_hex_to_int(
            tx :nonce :: STRING
        ) AS nonce,
        tx_block_index AS INDEX,
        tx :from :: STRING AS from_address,
        tx :to :: STRING AS to_address,
        tx :value / pow(
            10,
            18
        ) AS eth_value,
        tx :block_hash :: STRING AS block_hash,
        tx :gas_price / pow(
            10,
            9
        ) AS gas_price,
        tx :gas AS gas_limit,
        tx :input :: STRING AS DATA,
        CASE
            WHEN tx :receipt :status :: STRING = '0x1' THEN 'SUCCESS'
            ELSE 'FAIL'
        END AS status,
        silver_ethereum_2022.js_hex_to_int(
            tx :receipt :gasUsed :: STRING
        ) AS gas_used,
        silver_ethereum_2022.js_hex_to_int(
            tx :receipt :cumulativeGasUsed :: STRING
        ) AS cumulativeGasUsed,
        silver_ethereum_2022.js_hex_to_int(
            tx :receipt :effectiveGasPrice :: STRING
        ) AS effectiveGasPrice,
        (
            tx :gas_price * silver_ethereum_2022.js_hex_to_int(
                tx :receipt :gasUsed :: STRING
            )
        ) / pow(
            10,
            18
        ) AS tx_fee,
        ingested_at :: TIMESTAMP AS ingested_at
    FROM
        {{ ref('bronze_ethereum_2022__transactions') }}

{% if is_incremental() %}
WHERE
    ingested_at >= (
        SELECT
            MAX(
                ingested_at
            )
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    block_timestamp,
    block_number,
    tx_hash,
    nonce,
    INDEX,
    from_address,
    to_address,
    eth_value,
    block_hash,
    gas_price,
    gas_limit,
    DATA,
    status,
    gas_used,
    cumulativeGasUsed,
    effectiveGasPrice,
    tx_fee,
    ingested_at
FROM
    base_table qualify(ROW_NUMBER() over(PARTITION BY tx_hash
ORDER BY
    ingested_at DESC)) = 1
