-- depends_on: {{ ref('bronze__blocks') }}
{{ config(
    materialized = 'incremental',
    unique_key = "block_number",
    cluster_by = "block_timestamp::date",
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(hash,parent_hash,receipts_root,sha3_uncles,state_root,transactions_root,withdrawals_root)",
    full_refresh = false,
    tags = ['realtime']
) }}

SELECT
    block_number,
    utils.udf_hex_to_int(
        DATA :baseFeePerGas :: STRING
    ) :: INT AS base_fee_per_gas,
    utils.udf_hex_to_int(
        DATA :difficulty :: STRING
    ) :: INT AS difficulty,
    DATA :extraData :: STRING AS extra_data,
    utils.udf_hex_to_int(
        DATA :gasLimit :: STRING
    ) :: INT AS gas_limit,
    utils.udf_hex_to_int(
        DATA :gasUsed :: STRING
    ) :: INT AS gas_used,
    DATA :hash :: STRING AS HASH,
    DATA :logsBloom :: STRING AS logs_bloom,
    DATA :miner :: STRING AS miner,
    utils.udf_hex_to_int(
        DATA :nonce :: STRING
    ) :: INT AS nonce,
    utils.udf_hex_to_int(
        DATA :number :: STRING
    ) :: INT AS NUMBER,
    DATA :parentHash :: STRING AS parent_hash,
    DATA :receiptsRoot :: STRING AS receipts_root,
    DATA :sha3Uncles :: STRING AS sha3_uncles,
    utils.udf_hex_to_int(
        DATA :size :: STRING
    ) :: INT AS SIZE,
    DATA :stateRoot :: STRING AS state_root,
    utils.udf_hex_to_int(
        DATA :timestamp :: STRING
    ) :: TIMESTAMP AS block_timestamp,
    utils.udf_hex_to_int(
        DATA :totalDifficulty :: STRING
    ) :: INT AS total_difficulty,
    ARRAY_SIZE(
        DATA :transactions
    ) AS tx_count,
    DATA :transactionsRoot :: STRING AS transactions_root,
    DATA :uncles AS uncles,
    DATA :withdrawals AS withdrawals,
    DATA :withdrawalsRoot :: STRING AS withdrawals_root,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number']
    ) }} AS blocks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id,
    utils.udf_hex_to_int(
        DATA: blobGasUsed :: STRING
    ) :: INT AS blob_gas_used,
    utils.udf_hex_to_int(
        DATA: excessBlobGas :: STRING
    ) :: INT AS excess_blob_gas
FROM

{% if is_incremental() %}
{{ ref('bronze__blocks') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__blocks_fr') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
