{{ config(
    materialized = 'incremental',
    unique_key = "tx_hash",
    incremental_strategy = 'delete+insert',
    cluster_by = "_inserted_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH base AS (

    SELECT
        block_number,
        DATA,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('bronze__streamline_receipts') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
    AND _partition_by_block_number >= (
        SELECT
            MAX(_partition_by_block_number) - 100000 _partition_by_block_number
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
)
SELECT
    block_number,
    VALUE :blockHash :: STRING AS block_hash,
    PUBLIC.udf_hex_to_int(
        VALUE :blockNumber :: STRING
    ) :: INT AS blockNumber,
    PUBLIC.udf_hex_to_int(
        VALUE :cumulativeGasUsed :: STRING
    ) :: INT AS cumulative_gas_used,
    PUBLIC.udf_hex_to_int(
        VALUE :effectiveGasPrice :: STRING
    ) :: INT AS effective_gas_price,
    VALUE :from :: STRING AS from_address,
    PUBLIC.udf_hex_to_int(
        VALUE :gasUsed :: STRING
    ) :: INT AS gas_used,
    VALUE :logs AS logs,
    VALUE :logsBloom :: STRING AS logs_bloom,
    PUBLIC.udf_hex_to_int(
        VALUE :status :: STRING
    ) :: INT AS status,
    CASE
        WHEN status = 1 THEN TRUE
        ELSE FALSE
    END AS tx_success,
    CASE
        WHEN status = 1 THEN 'SUCCESS'
        ELSE 'FAILURE'
    END AS tx_status,
    VALUE :to :: STRING AS to_address,
    VALUE :transactionHash :: STRING AS tx_hash,
    PUBLIC.udf_hex_to_int(
        VALUE :transactionIndex :: STRING
    ) :: INT AS POSITION,
    PUBLIC.udf_hex_to_int(
        VALUE :type :: STRING
    ) :: INT AS TYPE,
    _partition_by_block_number,
    _inserted_timestamp
FROM
    base,
    LATERAL FLATTEN (
        input => DATA :result
    ) qualify(ROW_NUMBER() over (PARTITION BY block_number, tx_hash
ORDER BY
    _inserted_timestamp DESC)) = 1
