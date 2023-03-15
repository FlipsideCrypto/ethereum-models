{{ config(
    materialized = 'incremental',
    unique_key = "tx_hash",
    incremental_strategy = 'delete+insert',
    cluster_by = "ROUND(block_number, -3)"
) }}

WITH base AS (

    SELECT
        block_number,
        DATA,
        _inserted_timestamp
    FROM
        {{ ref('bronze__streamline_traces') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
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
        _inserted_timestamp
    FROM
        base,
        LATERAL FLATTEN (
            input => DATA :result
        )
),
receipt AS (
    SELECT
        A.block_number,
        block_timestamp,
        tx_hash,
        status
    FROM
        {{ ref('_tx_status') }} A
        JOIN (
            SELECT
                block_number
            FROM
                base
        ) b USING (block_number)
)
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
    _inserted_timestamp
FROM
    flat_data f
    JOIN receipt r USING (
        tx_hash,
        block_number
    ) qualify ROW_NUMBER() over (
        PARTITION BY tx_trace_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
