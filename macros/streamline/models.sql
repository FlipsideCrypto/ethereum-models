{% macro decode_logs_history(
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
            ) = 1
    )
SELECT
    l.block_number,
    l._log_id,
    abi.data AS abi,
    l.data
FROM
    {{ ref("streamline__decode_logs") }}
    l
    INNER JOIN {{ ref("silver__abis") }}
    abi
    ON l.abi_address = abi.contract_address
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

{% macro streamline_blocks_view(
        model
    ) %}
    WITH meta AS (
        SELECT
            last_modified AS _inserted_timestamp,
            CAST(
                SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER
            ) AS _partition_by_block_number
        FROM
            TABLE(
                information_schema.external_table_file_registration_history(
                    table_name => '{{ source( "bronze_streamline", model) }}'
                )
            ) A
    )
SELECT
    block_number,
    DATA,
    _partition_by_block_id AS _partition_by_block_number,
    _inserted_timestamp,
    MD5(
        CAST(COALESCE(CAST(block_number AS text), '') AS text)
    ) AS id
FROM
    {{ source(
        "bronze_streamline",
        model
    ) }}
    s
    JOIN meta b
    ON b._partition_by_block_number = s._partition_by_block_id
WHERE
    b._partition_by_block_number = s._partition_by_block_id
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
            '-32010'
        )
    )
{% endmacro %}
