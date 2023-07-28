-- depends_on: {{ ref('bronze__streamline_traces') }}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = "block_timestamp::date, _inserted_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    full_refresh = false,
    tags = ['realtime']
) }}

WITH traces_txs AS (

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
    _partition_by_block_id <= 2500000
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number, tx_position
ORDER BY
    _inserted_timestamp DESC)) = 1
),
base_table AS (
    SELECT
        CASE
            WHEN POSITION(
                '.',
                path :: STRING
            ) > 0 THEN REPLACE(
                REPLACE(
                    path :: STRING,
                    SUBSTR(path :: STRING, len(path :: STRING) - POSITION('.', REVERSE(path :: STRING)) + 1, POSITION('.', REVERSE(path :: STRING))),
                    ''
                ),
                '.',
                '__'
            )
            ELSE '__'
        END AS id,
        OBJECT_AGG(
            DISTINCT key,
            VALUE
        ) AS DATA,
        txs.tx_position AS tx_position,
        txs.block_number AS block_number,
        txs._inserted_timestamp AS _inserted_timestamp
    FROM
        traces_txs txs,
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
    GROUP BY
        tx_position,
        id,
        block_number,
        _inserted_timestamp
),
flattened_traces AS (
    SELECT
        DATA :from :: STRING AS from_address,
        utils.udf_hex_to_int(
            DATA :gas :: STRING
        ) AS gas,
        utils.udf_hex_to_int(
            DATA :gasUsed :: STRING
        ) AS gas_used,
        DATA :input :: STRING AS input,
        DATA :output :: STRING AS output,
        DATA :error :: STRING AS error_reason,
        DATA :to :: STRING AS to_address,
        DATA :type :: STRING AS TYPE,
        CASE
            WHEN DATA :type :: STRING = 'CALL' THEN utils.udf_hex_to_int(
                DATA :value :: STRING
            ) / pow(
                10,
                18
            )
            ELSE 0
        END AS eth_value,
        CASE
            WHEN id = '__' THEN CONCAT(
                DATA :type :: STRING,
                '_ORIGIN'
            )
            ELSE CONCAT(
                DATA :type :: STRING,
                '_',
                REPLACE(
                    REPLACE(REPLACE(REPLACE(id, 'calls', ''), '[', ''), ']', ''),
                    '__',
                    '_'
                )
            )
        END AS identifier,
        concat_ws(
            '-',
            block_number,
            tx_position,
            identifier
        ) AS _call_id,
        SPLIT(
            identifier,
            '_'
        ) AS id_split,
        ARRAY_SLICE(id_split, 1, ARRAY_SIZE(id_split)) AS levels,
        ARRAY_TO_STRING(
            levels,
            '_'
        ) AS LEVEL,
        CASE
            WHEN ARRAY_SIZE(levels) = 1
            AND levels [0] :: STRING = 'ORIGIN' THEN NULL
            WHEN ARRAY_SIZE(levels) = 1 THEN 'ORIGIN'
            ELSE ARRAY_TO_STRING(ARRAY_SLICE(levels, 0, ARRAY_SIZE(levels) -1), '_')END AS parent_level,
            COUNT(parent_level) over (
                PARTITION BY block_number,
                tx_position,
                parent_level
            ) AS sub_traces,*
            FROM
                base_table
        ),
        group_sub_traces AS (
            SELECT
                tx_position,
                block_number,
                parent_level,
                sub_traces
            FROM
                flattened_traces
            GROUP BY
                tx_position,
                block_number,
                parent_level,
                sub_traces
        ),
        add_sub_traces AS (
            SELECT
                flattened_traces.tx_position AS tx_position,
                flattened_traces.block_number :: INTEGER AS block_number,
                flattened_traces.error_reason AS error_reason,
                flattened_traces.from_address AS from_address,
                flattened_traces.to_address AS to_address,
                flattened_traces.eth_value :: FLOAT AS eth_value,
                flattened_traces.gas :: FLOAT AS gas,
                flattened_traces.gas_used :: FLOAT AS gas_used,
                flattened_traces.input AS input,
                flattened_traces.output AS output,
                flattened_traces.type AS TYPE,
                flattened_traces.identifier AS identifier,
                flattened_traces._call_id AS _call_id,
                flattened_traces.data AS DATA,
                group_sub_traces.sub_traces AS sub_traces,
                ROW_NUMBER() over(
                    PARTITION BY flattened_traces.block_number,
                    flattened_traces.tx_position
                    ORDER BY
                        flattened_traces.gas :: FLOAT DESC,
                        flattened_traces.eth_value :: FLOAT ASC,
                        flattened_traces.to_address
                ) AS trace_index,
                flattened_traces._inserted_timestamp AS _inserted_timestamp
            FROM
                flattened_traces
                LEFT OUTER JOIN group_sub_traces
                ON flattened_traces.tx_position = group_sub_traces.tx_position
                AND flattened_traces.level = group_sub_traces.parent_level
                AND flattened_traces.block_number = group_sub_traces.block_number
        ),
        final_traces AS (
            SELECT
                tx_position,
                trace_index,
                block_number,
                error_reason,
                from_address,
                to_address,
                eth_value,
                gas,
                gas_used,
                input,
                output,
                TYPE,
                identifier,
                _call_id,
                _inserted_timestamp,
                DATA,
                sub_traces
            FROM
                add_sub_traces
            WHERE
                identifier IS NOT NULL
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
                f.eth_value,
                f.gas,
                f.gas_used,
                f.input,
                f.output,
                f.type,
                f.identifier,
                f.sub_traces,
                f.error_reason,
                CASE
                    WHEN f.error_reason IS NULL THEN 'SUCCESS'
                    ELSE 'FAIL'
                END AS trace_status,
                f.data,
                CASE
                    WHEN t.tx_hash IS NULL
                    OR t.block_timestamp IS NULL
                    OR t.tx_status IS NULL THEN TRUE
                    ELSE FALSE
                END AS is_pending,
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
        txs.tx_hash,
        txs.block_timestamp,
        txs.tx_status,
        t.tx_position,
        t.trace_index,
        t.from_address,
        t.to_address,
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
