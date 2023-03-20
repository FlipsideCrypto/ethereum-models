{{ config(
    materialized = 'incremental',
    unique_key = "tx_hash",
    cluster_by = "block_timestamp::date, _inserted_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH new_records AS (

    SELECT
        A.block_number AS block_number,
        A.data :blockHash :: STRING AS block_hash,
        PUBLIC.udf_hex_to_int(
            A.data :blockNumber :: STRING
        ) :: INT AS blockNumber,
        PUBLIC.udf_hex_to_int(
            A.data :chainId :: STRING
        ) :: INT AS chain_id,
        A.data :from :: STRING AS from_address,
        PUBLIC.udf_hex_to_int(
            A.data :gas :: STRING
        ) :: INT AS gas,
        PUBLIC.udf_hex_to_int(
            A.data :gasPrice :: STRING
        ) :: INT AS gas_price,
        A.data :hash :: STRING AS tx_hash,
        A.data :input :: STRING AS input_data,
        SUBSTR(
            input_data,
            1,
            10
        ) AS origin_function_signature,
        PUBLIC.udf_hex_to_int(
            A.data :nonce :: STRING
        ) :: INT AS nonce,
        A.data :r :: STRING AS r,
        A.data :s :: STRING AS s,
        A.data :to :: STRING AS to_address,
        PUBLIC.udf_hex_to_int(
            A.data :transactionIndex :: STRING
        ) :: INT AS POSITION,
        A.data :type :: STRING AS TYPE,
        A.data :v :: STRING AS v,
        PUBLIC.udf_hex_to_int(
            A.data :value :: STRING
        ) / pow(
            10,
            18
        ) :: FLOAT AS VALUE,
        block_timestamp,
        CASE
            WHEN block_timestamp IS NULL
            OR tx_status IS NULL THEN TRUE
            ELSE FALSE
        END AS is_pending,
        r.gas_used,
        tx_success,
        tx_status,
        cumulative_gas_used,
        effective_gas_price,
        A._partition_by_block_number,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('bronze__streamline_transactions') }} A
        LEFT OUTER JOIN {{ ref('silver__streamline_receipts') }}
        r
        ON A.block_number = r.block_number
        AND A.data :hash :: STRING = r.tx_hash
        LEFT OUTER JOIN {{ ref('silver__streamline_blocks') }}
        b
        ON A.block_number = b.block_number

{% if is_incremental() %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
    AND _partition_by_block_number >= ({% if var('STREAMLINE_RUN_HISTORY') %}
    SELECT
        0 AS _partition_by_block_number
    {% else %}
    SELECT
        MAX(_partition_by_block_number) - 100000 _partition_by_block_number
    FROM
        {{ this }}
    {% endif %})
{% endif %})

{% if is_incremental() %},
missing_data AS (
    SELECT
        t.block_number,
        t.block_hash,
        t.chain_id,
        t.from_address,
        t.gas,
        t.gas_price,
        t.tx_hash,
        t.input_data,
        t.origin_function_signature,
        t.nonce,
        t.r,
        t.s,
        t.to_address,
        t.position,
        t.type,
        t.v,
        t.value,
        b.block_timestamp,
        FALSE AS is_pending,
        r.gas_used,
        r.tx_success,
        r.tx_status,
        r.cumulative_gas_used,
        r.effective_gas_price,
        t._partition_by_block_number,
        GREATEST(
            t._inserted_timestamp,
            b._inserted_timestamp,
            r._inserted_timestamp
        ) AS _inserted_timestamp
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('silver__streamline_blocks') }}
        b
        ON t.block_number = b.block_number
        INNER JOIN {{ ref('silver__streamline_receipts') }}
        r
        ON t.tx_hash = r.tx_hash
        AND t.block_number = r.block_number
    WHERE
        t.is_pending
)
{% endif %}
SELECT
    block_number,
    block_hash,
    chain_id,
    from_address,
    gas,
    gas_price,
    tx_hash,
    input_data,
    origin_function_signature,
    nonce,
    r,
    s,
    to_address,
    POSITION,
    TYPE,
    v,
    VALUE,
    block_timestamp,
    is_pending,
    gas_used,
    tx_success,
    tx_status,
    cumulative_gas_used,
    effective_gas_price,
    _partition_by_block_number,
    _inserted_timestamp
FROM
    new_records qualify(ROW_NUMBER() over (PARTITION BY block_number, tx_hash
ORDER BY
    _inserted_timestamp DESC)) = 1

{% if is_incremental() %}
UNION
SELECT
    *
FROM
    missing_data
{% endif %}
