{{ config(
    materialized = 'incremental',
    unique_key = "tx_hash",
    incremental_strategy = 'delete+insert',
    cluster_by = "block_timestamp::date, _inserted_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH base AS (

    SELECT
        block_number,
        DATA,
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        {{ ref('bronze__streamline_traces') }}

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
),
flat_data AS (
    SELECT
        block_number,
        VALUE :transactionHash :: STRING AS tx_hash,
        INDEX AS array_index,
        VALUE :type :: STRING AS callType,
        VALUE :action :callType :: STRING AS callType1,
        VALUE :action :from :: STRING AS from_address,
        VALUE :action :gas :: STRING AS gas,
        VALUE :action :input :: STRING AS input,
        VALUE :action :to :: STRING AS to_address,
        VALUE :action :value :: STRING AS eth_value,
        VALUE :blockHash :: STRING AS blockHash,
        VALUE :result :gasUsed :: STRING AS gas_used,
        VALUE :result :output :: STRING AS output,
        VALUE :subtraces :: INT AS sub_traces,
        VALUE :traceAddress AS traceAddress,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                array_index ASC
        ) AS trace_index,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        base,
        LATERAL FLATTEN (
            input => DATA :result
        )
),
new_records AS (
    SELECT
        f.block_number,
        block_timestamp,
        status AS tx_status,
        f.tx_hash,
        trace_index,
        UPPER(callType) AS tx_type,
        UPPER(callType1) AS TYPE,
        from_address,
        PUBLIC.udf_hex_to_int(gas) :: INT AS gas,
        input,
        to_address,
        PUBLIC.udf_hex_to_int(eth_value) / pow (
            10,
            18
        ) :: FLOAT AS eth_value,
        blockHash,
        PUBLIC.udf_hex_to_int(gas_used) :: INT AS gas_used,
        output,
        sub_traces,
        REPLACE(
            REPLACE(REPLACE(traceAddress :: STRING, '['), ']'),
            ',',
            '_'
        ) AS trace_address,
        CONCAT(
            TYPE,
            '_',
            CASE
                WHEN trace_index = 1 THEN 'ORIGIN'
                ELSE trace_address
            END
        ) AS identifier,
        CONCAT(
            f.tx_hash,
            '_',
            trace_index
        ) AS tx_trace_id,
        _inserted_timestamp,
        _partition_by_block_number,
        tx_status,
        tx_success,
        CASE
            WHEN block_timestamp IS NULL
            OR tx_status IS NULL THEN TRUE
            ELSE FALSE
        END AS is_pending
    FROM
        flat_data f
        LEFT OUTER JOIN {{ ref('bronze__streamline_receipts') }}
        r
        ON f.block_number = r.block_number
        AND f.data :hash :: STRING = r.tx_hash
        LEFT OUTER JOIN {{ ref('bronze__streamline_blocks') }}
        b
        ON f.block_number = b.block_number
)

{% if is_incremental() %},
missing_data AS (
    SELECT
        t.block_number,
        b.block_timestamp,
        t.tx_hash,
        t.trace_index,
        t.tx_type,
        t.type,
        t.from_address,
        t.gas,
        t.input,
        t.to_address,
        t.eth_value,
        t.blockHash,
        t.gas_used,
        t.output,
        t.sub_traces,
        t.trace_address,
        t.identifier,
        t.tx_trace_id,
        GREATEST(
            t._inserted_timestamp,
            b._inserted_timestamp,
            r._inserted_timestamp
        ) AS _inserted_timestamp,
        t._partition_by_block_number,
        r.tx_status,
        r.tx_success,
        FALSE AS is_pending
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('bronze__streamline_blocks') }}
        b
        ON t.block_number = b.block_number
        INNER JOIN {{ ref('bronze__streamline_receipts') }}
        r
        ON t.tx_hash = r.tx_hash
        AND t.block_number = r.block_number
    WHERE
        t.is_pending
)
{% endif %}
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    trace_index,
    tx_type,
    TYPE,
    from_address,
    gas,
    input,
    to_address,
    eth_value,
    blockHash,
    gas_used,
    output,
    sub_traces,
    trace_address,
    identifier,
    tx_trace_id,
    _inserted_timestamp,
    _partition_by_block_number,
    tx_status,
    tx_success,
    is_pending
FROM
    new_records qualify ROW_NUMBER() over (
        PARTITION BY tx_trace_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1

{% if is_incremental() %}
UNION
SELECT
    *
FROM
    missing_data
{% endif %}
