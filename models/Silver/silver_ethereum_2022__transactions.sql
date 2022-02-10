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
        tx :nonce :: STRING AS nonce,
        tx_block_index AS INDEX,
        tx :from :: STRING AS from_address,
        tx :to :: STRING AS to_address,
        tx :value AS VALUE,
        tx :block_hash :: STRING AS block_hash,
        tx :gas_price AS gas_price,
        tx :gas AS gas,
        tx :input :: STRING AS DATA,
        tx :receipt :status :: STRING = '0x1' AS status,
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
    VALUE,
    block_hash,
    gas_price,
    gas,
    DATA,
    status,
    ingested_at
FROM
    base_table qualify(ROW_NUMBER() over(PARTITION BY tx_hash
ORDER BY
    ingested_at DESC)) = 1
