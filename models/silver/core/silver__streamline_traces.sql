{{ config(
    materialized = 'incremental',
    unique_key = "tx_hash",
    incremental_strategy = 'delete+insert',
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH meta AS (

    SELECT
        registered_on,
        last_modified,
        LEAST(
            last_modified,
            registered_on
        ) AS _inserted_timestamp,
        file_name
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "traces") }}'
            )
        ) A

{% if is_incremental() %}
WHERE
    LEAST(
        registered_on,
        last_modified
    ) >= (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }})
    ),
    partitions AS (
        SELECT
            DISTINCT CAST(
                SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER
            ) AS _partition_by_block_number
        FROM
            meta
    ),
    max_date AS (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }})
        {% else %}
    )
{% endif %},
base AS (
    SELECT
        block_number,
        DATA,
        last_modified AS _inserted_timestamp
    FROM
        {{ source(
            "bronze_streamline",
            "traces"
        ) }}
        s
        JOIN meta b
        ON b.file_name = metadata$filename

{% if is_incremental() %}
JOIN partitions p
ON p._partition_by_block_number = s._partition_by_block_id
{% endif %}
WHERE
    (DATA :error :code IS NULL
    OR DATA :error :code NOT IN (
        '-32000',
        '-32001',
        '-32002',
        '-32003',
        '-32004',
        '-32005',
        '-32006',
        '-32007',
        '-32008',
        '-32009',
        '-32010'
    ))
{% if is_incremental() %}
and
    b.last_modified > (
        SELECT
            max_INSERTED_TIMESTAMP
        FROM
            max_date
    )
{% endif %}
qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
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
