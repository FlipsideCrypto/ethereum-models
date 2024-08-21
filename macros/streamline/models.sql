{% macro decode_logs_history(
        start,
        stop
    ) %}
    WITH look_back AS (
        SELECT
            block_number
        FROM
            {{ ref("_24_hour_lookback") }}
    )
SELECT
    l.block_number,
    l._log_id,
    A.abi AS abi,
    OBJECT_CONSTRUCT(
        'topics',
        l.topics,
        'data',
        l.data,
        'address',
        l.contract_address
    ) AS DATA
FROM
    {{ ref("silver__logs") }}
    l
    INNER JOIN {{ ref("silver__complete_event_abis") }} A
    ON A.parent_contract_address = l.contract_address
    AND A.event_signature = l.topics [0] :: STRING
    AND l.block_number BETWEEN A.start_block
    AND A.end_block
WHERE
    (
        l.block_number BETWEEN {{ start }}
        AND {{ stop }}
    )
    AND l.block_number <= (
        SELECT
            block_number
        FROM
            look_back
    )
    AND _log_id NOT IN (
        SELECT
            _log_id
        FROM
            {{ ref("streamline__complete_decode_logs") }}
        WHERE
            (
                block_number BETWEEN {{ start }}
                AND {{ stop }}
            )
            AND block_number <= (
                SELECT
                    block_number
                FROM
                    look_back
            )
    )
{% endmacro %}

{% macro decode_traces_history(
        start,
        stop
    ) %}
    WITH look_back AS (
        SELECT
            block_number
        FROM
            {{ ref("_24_hour_lookback") }}
    )
SELECT
    t.block_number,
    t.tx_hash,
    t.trace_index,
    _call_id,
    A.abi AS abi,
    A.function_name AS function_name,
    CASE
        WHEN TYPE = 'DELEGATECALL' THEN from_address
        ELSE to_address
    END AS abi_address,
    t.input AS input,
    COALESCE(
        t.output,
        '0x'
    ) AS output
FROM
    {{ ref("silver__traces") }}
    t
    INNER JOIN {{ ref("silver__complete_function_abis") }} A
    ON A.parent_contract_address = abi_address
    AND LEFT(
        t.input,
        10
    ) = LEFT(
        A.function_signature,
        10
    )
    AND t.block_number BETWEEN A.start_block
    AND A.end_block
WHERE
    (
        t.block_number BETWEEN {{ start }}
        AND {{ stop }}
    )
    AND t.block_number < (
        SELECT
            block_number
        FROM
            look_back
    )
    AND t.block_number IS NOT NULL
    AND _call_id NOT IN (
        SELECT
            _call_id
        FROM
            {{ ref("streamline__complete_decode_traces") }}
        WHERE
            (
                block_number BETWEEN {{ start }}
                AND {{ stop }}
            )
            AND block_number < (
                SELECT
                    block_number
                FROM
                    look_back
            )
    )
{% endmacro %}

{% macro streamline_external_table_query(
        model,
        partition_function,
        partition_name,
        unique_key
    ) %}
    WITH meta AS (
        SELECT
            last_modified AS _inserted_timestamp,
            file_name,
            {{ partition_function }} AS {{ partition_name }}
        FROM
            TABLE(
                information_schema.external_table_file_registration_history(
                    start_time => DATEADD('day', -3, CURRENT_TIMESTAMP()),
                    table_name => '{{ source( "bronze_streamline", model) }}')
                ) A
            )
        SELECT
            {{ unique_key }},
            DATA,
            _inserted_timestamp,
            MD5(
                CAST(
                    COALESCE(CAST({{ unique_key }} AS text), '' :: STRING) AS text
                )
            ) AS id,
            s.{{ partition_name }},
            s.value AS VALUE
        FROM
            {{ source(
                "bronze_streamline",
                model
            ) }}
            s
            JOIN meta b
            ON b.file_name = metadata$filename
            AND b.{{ partition_name }} = s.{{ partition_name }}
        WHERE
            b.{{ partition_name }} = s.{{ partition_name }}
            AND (
                DATA :error :code IS NULL
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
                    '-32010',
                    '-32608'
                )
            )
{% endmacro %}

{% macro streamline_external_table_FR_query(
        model,
        partition_function,
        partition_name,
        unique_key
    ) %}
    WITH meta AS (
        SELECT
            registered_on AS _inserted_timestamp,
            file_name,
            {{ partition_function }} AS {{ partition_name }}
        FROM
            TABLE(
                information_schema.external_table_files(
                    table_name => '{{ source( "bronze_streamline", model) }}'
                )
            ) A
    )
SELECT
    {{ unique_key }},
    DATA,
    _inserted_timestamp,
    MD5(
        CAST(
            COALESCE(CAST({{ unique_key }} AS text), '' :: STRING) AS text
        )
    ) AS id,
    s.{{ partition_name }},
    s.value AS VALUE
FROM
    {{ source(
        "bronze_streamline",
        model
    ) }}
    s
    JOIN meta b
    ON b.file_name = metadata$filename
    AND b.{{ partition_name }} = s.{{ partition_name }}
WHERE
    b.{{ partition_name }} = s.{{ partition_name }}
    AND (
        DATA :error :code IS NULL
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
            '-32010',
            '-32608'
        )
    )
{% endmacro %}

{% macro native_balances_history(
        start,
        stop
    ) %}
    WITH look_back AS (
        SELECT
            block_number
        FROM
            {{ ref("_max_block_by_date") }}
            qualify ROW_NUMBER() over (
                ORDER BY
                    block_number DESC
            ) = 3
    ),
    traces AS (
        SELECT
            block_number,
            from_address,
            to_address
        FROM
            {{ ref('silver__traces') }}
        WHERE
            eth_value > 0
            AND block_number < (
                SELECT
                    block_number
                FROM
                    look_back
            )
            AND block_number BETWEEN {{ start }}
            AND {{ stop }}
    ),
    stacked AS (
        SELECT
            DISTINCT block_number,
            from_address AS address
        FROM
            traces
        WHERE
            from_address IS NOT NULL
            AND from_address <> '0x0000000000000000000000000000000000000000'
        UNION
        SELECT
            DISTINCT block_number,
            to_address AS address
        FROM
            traces
        WHERE
            to_address IS NOT NULL
            AND to_address <> '0x0000000000000000000000000000000000000000'
    )
SELECT
    block_number,
    address
FROM
    stacked
WHERE
    block_number IS NOT NULL
EXCEPT
SELECT
    block_number,
    address
FROM
    {{ ref("streamline__complete_eth_balances") }}
WHERE
    block_number < (
        SELECT
            block_number
        FROM
            look_back
    )
    AND block_number BETWEEN {{ start }}
    AND {{ stop }}
{% endmacro %}
