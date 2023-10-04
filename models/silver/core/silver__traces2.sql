-- depends_on: {{ ref('bronze__streamline_traces') }}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = "block_timestamp::date, _inserted_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH bronze_traces AS (

    SELECT
        block_number,
        VALUE :array_index :: INT AS tx_position,
        DATA :result AS full_traces,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_traces') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_FR_traces') }}
WHERE
    _partition_by_block_id <= 2300000
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number, tx_position
ORDER BY
    _inserted_timestamp DESC)) = 1
),
flatten_traces AS (
    SELECT
        block_number,
        tx_position,
        IFF(
            path IN (
                'result',
                'result.value',
                'result.type',
                'result.to',
                'result.input',
                'result.gasUsed',
                'result.gas',
                'result.from',
                'result.output',
                'result.error',
                'result.revertReason',
                'gasUsed',
                'gas',
                'type',
                'to',
                'from',
                'value',
                'input',
                'error',
                'output',
                'revertReason'
            ),
            'ORIGIN',
            REGEXP_REPLACE(REGEXP_REPLACE(path, '[^0-9]+', '_'), '^_|_$', '')
        ) AS trace_address,
        _inserted_timestamp,
        OBJECT_AGG(
            key,
            VALUE
        ) AS trace_json,
        CASE
            WHEN trace_address = 'ORIGIN' THEN NULL
            WHEN POSITION(
                '_' IN trace_address
            ) = 0 THEN 'ORIGIN'
            ELSE REGEXP_REPLACE(
                trace_address,
                '_[0-9]+$',
                '',
                1,
                1
            )
        END AS parent_trace_address,
        SPLIT(
            trace_address,
            '_'
        ) AS str_array
    FROM
        bronze_traces txs,
        TABLE(
            FLATTEN(
                input => PARSE_JSON(
                    txs.full_traces
                ),
                recursive => TRUE
            )
        ) f
    WHERE
        f.index IS NULL
        AND f.key != 'calls'
        AND f.path != 'result'
    GROUP BY
        block_number,
        tx_position,
        trace_address,
        _inserted_timestamp
),
sub_traces AS (
    SELECT
        block_number,
        tx_position,
        parent_trace_address,
        COUNT(*) AS sub_traces
    FROM
        flatten_traces
    GROUP BY
        block_number,
        tx_position,
        parent_trace_address
),
num_array AS (
    SELECT
        block_number,
        tx_position,
        trace_address,
        ARRAY_AGG(flat_value) AS num_array
    FROM
        (
            SELECT
                block_number,
                tx_position,
                trace_address,
                IFF(
                    VALUE :: STRING = 'ORIGIN',
                    -1,
                    VALUE :: INT
                ) AS flat_value
            FROM
                flatten_traces,
                LATERAL FLATTEN (
                    input => str_array
                )
        )
    GROUP BY
        block_number,
        tx_position,
        trace_address
),
cleaned_traces AS (
    SELECT
        b.block_number,
        b.tx_position,
        b.trace_address,
        IFNULL(
            sub_traces,
            0
        ) AS sub_traces,
        num_array,
        ROW_NUMBER() over (
            PARTITION BY b.block_number,
            b.tx_position
            ORDER BY
                num_array ASC
        ) - 1 AS trace_index,
        trace_json,
        b._inserted_timestamp
    FROM
        flatten_traces b
        LEFT JOIN sub_traces s
        ON b.block_number = s.block_number
        AND b.tx_position = s.tx_position
        AND b.trace_address = s.parent_trace_address
        JOIN num_array n
        ON b.block_number = n.block_number
        AND b.tx_position = n.tx_position
        AND b.trace_address = n.trace_address
),
final_traces AS (
    SELECT
        tx_position,
        trace_index,
        block_number,
        trace_address,
        trace_json :error :: STRING AS error_reason,
        trace_json :from :: STRING AS from_address,
        trace_json :to :: STRING AS to_address,
        utils.udf_decimal_adjust(
            utils.udf_hex_to_int(
                IFF(
                    trace_json :value IS NULL,
                    '0x0',
                    trace_json :value :: STRING
                )
            ),
            18
        ) AS eth_value_precise,
        eth_value_precise :: FLOAT AS eth_value,
        utils.udf_hex_to_int(
            trace_json :gas :: STRING
        ) :: INT AS gas,
        utils.udf_hex_to_int(
            trace_json :gasUsed :: STRING
        ) :: INT AS gas_used,
        trace_json :input :: STRING AS input,
        trace_json :output :: STRING AS output,
        trace_json :type :: STRING AS TYPE,
        concat_ws(
            '_',
            TYPE,
            trace_address
        ) AS identifier,
        concat_ws(
            '_',
            block_number,
            tx_position,
            identifier
        ) AS _call_id,
        _inserted_timestamp,
        trace_json AS DATA,
        sub_traces
    FROM
        cleaned_traces
),
new_records AS (
    SELECT
        f.block_number,
        t.tx_hash,
        t.block_timestamp,
        t.tx_status,
        f.tx_position,
        f.trace_index,
        f.from_address,
        f.to_address,
        f.eth_value_precise,
        f.eth_value,
        f.gas,
        f.gas_used,
        f.input,
        f.output,
        f.type,
        f.identifier,
        f.sub_traces,
        f.error_reason,
        IFF(
            f.error_reason IS NULL,
            'SUCCESS',
            'FAIL'
        ) AS trace_status,
        f.data,
        IFF(
            t.tx_hash IS NULL
            OR t.block_timestamp IS NULL
            OR t.tx_status IS NULL,
            TRUE,
            FALSE
        ) AS is_pending,
        f._call_id,
        f._inserted_timestamp
    FROM
        final_traces f
        LEFT OUTER JOIN {{ ref('silver__transactions') }}
        t
        ON f.tx_position = t.position
        AND f.block_number = t.block_number

{% if is_incremental() %}
AND t._INSERTED_TIMESTAMP >= (
    SELECT
        DATEADD('hour', -24, MAX(_inserted_timestamp))
    FROM
        {{ this }})
    {% endif %}
)

{% if is_incremental() %},
missing_data AS (
    SELECT
        t.block_number,
        txs.tx_hash,
        txs.block_timestamp,
        txs.tx_status,
        t.tx_position,
        t.trace_index,
        t.from_address,
        t.to_address,
        t.eth_value_precise,
        t.eth_value,
        t.gas,
        t.gas_used,
        t.input,
        t.output,
        t.type,
        t.identifier,
        t.sub_traces,
        t.error_reason,
        t.trace_status,
        t.data,
        FALSE AS is_pending,
        t._call_id,
        GREATEST(
            t._inserted_timestamp,
            txs._inserted_timestamp
        ) AS _inserted_timestamp
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('silver__transactions') }}
        txs
        ON t.tx_position = txs.position
        AND t.block_number = txs.block_number
    WHERE
        t.is_pending
)
{% endif %},
FINAL AS (
    SELECT
        block_number,
        tx_hash,
        block_timestamp,
        tx_status,
        tx_position,
        trace_index,
        from_address,
        to_address,
        eth_value_precise,
        eth_value,
        gas,
        gas_used,
        input,
        output,
        TYPE,
        identifier,
        sub_traces,
        error_reason,
        trace_status,
        DATA,
        is_pending,
        _call_id,
        _inserted_timestamp
    FROM
        new_records

{% if is_incremental() %}
UNION
SELECT
    block_number,
    tx_hash,
    block_timestamp,
    tx_status,
    tx_position,
    trace_index,
    from_address,
    to_address,
    eth_value_precise,
    eth_value,
    gas,
    gas_used,
    input,
    output,
    TYPE,
    identifier,
    sub_traces,
    error_reason,
    trace_status,
    DATA,
    is_pending,
    _call_id,
    _inserted_timestamp
FROM
    missing_data
{% endif %}
)
SELECT
    *
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY block_number, tx_position, trace_index
ORDER BY
    _inserted_timestamp DESC, is_pending ASC)) = 1
