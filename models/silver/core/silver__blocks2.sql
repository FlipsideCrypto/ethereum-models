-- depends_on: {{ ref('bronze__streamline_blocks') }}
{{ config(
    materialized = 'incremental',
    unique_key = "block_number",
    cluster_by = "block_timestamp::date"
) }}

SELECT
    block_number,
    PUBLIC.udf_hex_to_int(
        DATA :result :baseFeePerGas :: STRING
    ) :: INT AS base_fee_per_gas,
    PUBLIC.udf_hex_to_int(
        DATA :result :difficulty :: STRING
    ) :: INT AS difficulty,
    DATA :result :extraData :: STRING AS extra_data,
    PUBLIC.udf_hex_to_int(
        DATA :result :gasLimit :: STRING
    ) :: INT AS gas_limit,
    PUBLIC.udf_hex_to_int(
        DATA :result :gasUsed :: STRING
    ) :: INT AS gas_used,
    DATA :result :hash :: STRING AS HASH,
    DATA :result :logsBloom :: STRING AS logs_bloom,
    DATA :result :miner :: STRING AS miner,
    PUBLIC.udf_hex_to_int(
        DATA :result :nonce :: STRING
    ) :: INT AS nonce,
    PUBLIC.udf_hex_to_int(
        DATA :result :number :: STRING
    ) :: INT AS NUMBER,
    DATA :result :parentHash :: STRING AS parent_hash,
    DATA :result :receiptsRoot :: STRING AS receipts_root,
    DATA :result :sha3Uncles :: STRING AS sha3_uncles,
    PUBLIC.udf_hex_to_int(
        DATA :result :size :: STRING
    ) :: INT AS SIZE,
    DATA :result :stateRoot :: STRING AS state_root,
    PUBLIC.udf_hex_to_int(
        DATA :result :timestamp :: STRING
    ) :: TIMESTAMP AS block_timestamp,
    PUBLIC.udf_hex_to_int(
        DATA :result :totalDifficulty :: STRING
    ) :: INT AS total_difficulty,
    ARRAY_SIZE(
        DATA :result :transactions
    ) AS tx_count,
    DATA :result :transactionsRoot :: STRING AS transactions_root,
    DATA :result :uncles AS uncles,
    DATA :result :withdrawals AS withdrawals,
    DATA :result :withdrawalsRoot :: STRING AS withdrawals_root,
    _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_blocks') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_FR_blocks') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
