-- depends_on: {{ ref('bronze__streamline_transactions') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = "block_timestamp::date, _inserted_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['realtime']
) }}

WITH base AS (

    SELECT
        block_number,
        DATA,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_transactions') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
    AND IS_OBJECT(DATA)
{% else %}
    {{ ref('bronze__streamline_FR_transactions') }}
WHERE
    IS_OBJECT(DATA)
{% endif %}
),
base_tx AS (
    SELECT
        A.block_number AS block_number,
        A.data :blockHash :: STRING AS block_hash,
        utils.udf_hex_to_int(
            A.data :blockNumber :: STRING
        ) :: INT AS blockNumber,
        utils.udf_hex_to_int(
            A.data :chainId :: STRING
        ) :: INT AS chain_id,
        A.data :from :: STRING AS from_address,
        utils.udf_hex_to_int(
            A.data :gas :: STRING
        ) :: INT AS gas,
        utils.udf_hex_to_int(
            A.data :gasPrice :: STRING
        ) :: INT / pow(
            10,
            9
        ) AS gas_price,
        A.data :hash :: STRING AS tx_hash,
        A.data :input :: STRING AS input_data,
        SUBSTR(
            input_data,
            1,
            10
        ) AS origin_function_signature,
        utils.udf_hex_to_int(
            A.data :maxFeePerGas :: STRING
        ) :: INT / pow(
            10,
            9
        ) AS max_fee_per_gas,
        utils.udf_hex_to_int(
            A.data :maxPriorityFeePerGas :: STRING
        ) :: INT / pow(
            10,
            9
        ) AS max_priority_fee_per_gas,
        utils.udf_hex_to_int(
            A.data :nonce :: STRING
        ) :: INT AS nonce,
        A.data :r :: STRING AS r,
        A.data :s :: STRING AS s,
        A.data :to :: STRING AS to_address1,
        CASE
            WHEN to_address1 = '' THEN NULL
            ELSE to_address1
        END AS to_address,
        utils.udf_hex_to_int(
            A.data :transactionIndex :: STRING
        ) :: INT AS POSITION,
        A.data :type :: STRING AS TYPE,
        A.data :v :: STRING AS v,
        utils.udf_hex_to_int(
            A.data :value :: STRING
        ) / pow(
            10,
            18
        ) :: FLOAT AS VALUE,
        A.data :accessList AS access_list,
        A._INSERTED_TIMESTAMP
    FROM
        base A
),
new_records AS (
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
        t.max_fee_per_gas,
        t.max_priority_fee_per_gas,
        t.nonce,
        t.r,
        t.s,
        t.to_address1,
        t.to_address,
        t.position,
        t.type,
        t.v,
        t.value,
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
        (
            gas_price * r.gas_used
        ) / pow(
            10,
            9
        ) AS tx_fee,
        r.type AS tx_type,
        t.access_list,
        t._inserted_timestamp
    FROM
        base_tx t
        LEFT OUTER JOIN {{ ref('silver__blocks') }}
        b
        ON t.block_number = b.block_number
        LEFT OUTER JOIN {{ ref('silver__receipts') }}
        r
        ON t.block_number = r.block_number
        AND t.tx_hash = r.tx_hash

{% if is_incremental() %}
AND r._INSERTED_TIMESTAMP >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
)

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
        t.max_fee_per_gas,
        t.max_priority_fee_per_gas,
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
        (
            t.gas_price * r.gas_used
        ) / pow(
            10,
            9
        ) AS tx_fee,
        r.type AS tx_type,
        t.access_list,
        GREATEST(
            t._inserted_timestamp,
            b._inserted_timestamp,
            r._inserted_timestamp
        ) AS _inserted_timestamp
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('silver__blocks') }}
        b
        ON t.block_number = b.block_number
        INNER JOIN {{ ref('silver__receipts') }}
        r
        ON t.tx_hash = r.tx_hash
        AND t.block_number = r.block_number
    WHERE
        t.is_pending
)
{% endif %},
FINAL AS (
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
        max_fee_per_gas,
        max_priority_fee_per_gas,
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
        tx_fee,
        tx_type,
        access_list,
        _inserted_timestamp
    FROM
        new_records

{% if is_incremental() %}
UNION
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
    max_fee_per_gas,
    max_priority_fee_per_gas,
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
    tx_fee,
    tx_type,
    access_list,
    _inserted_timestamp
FROM
    missing_data
{% endif %}
)
SELECT
    *
FROM
    FINAL qualify(ROW_NUMBER() over (PARTITION BY block_number, POSITION
ORDER BY
    _inserted_timestamp DESC, is_pending ASC)) = 1
