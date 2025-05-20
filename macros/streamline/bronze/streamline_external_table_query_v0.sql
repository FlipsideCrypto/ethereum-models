{% macro v0_streamline_external_table_query(
        model,
        partition_function,
        balances = false,
        block_number = true
    ) %}
    WITH meta AS (
        SELECT
            job_created_time AS _inserted_timestamp,
            file_name,
            {{ partition_function }} AS partition_key
        FROM
            TABLE(
                information_schema.external_table_file_registration_history(
                    start_time => DATEADD('day', -3, CURRENT_TIMESTAMP()),
                    table_name => '{{ source( "bronze_streamline", model) }}')
                ) A
            )
        SELECT
            s.*,
            b.file_name,
            b._inserted_timestamp

            {% if balances %},
            r.block_timestamp :: TIMESTAMP AS block_timestamp
        {% endif %}

        {% if block_number %},
            COALESCE(
                TRY_TO_NUMBER(s.value :"BLOCK_NUMBER" :: STRING),
                TRY_TO_NUMBER(s.metadata :request :"data" :id :: STRING),
                TRY_TO_NUMBER(PARSE_JSON(
                    s.metadata :request :"data"
                ) :id :: STRING)
            ) AS block_number
        {% endif %}
        FROM
            {{ source(
                "bronze_streamline",
                model
            ) }}
            s
            JOIN meta b
            ON b.file_name = metadata$filename
            AND b.partition_key = s.partition_key

            {% if balances %}
            JOIN {{ ref('_block_ranges') }}
            r
            ON r.block_number = COALESCE(
                TRY_TO_NUMBER(s.value :"BLOCK_NUMBER" :: STRING),
                TRY_TO_NUMBER(s.value :"block_number" :: STRING)
            )
        {% endif %}
        WHERE
            b.partition_key = s.partition_key
            AND DATA :error IS NULL
            AND DATA IS NOT NULL
{% endmacro %}

{% macro v0_streamline_external_table_fr_query(
        model,
        partition_function,
        partition_join_key = "partition_key",
        balances = false,
        block_number = true
    ) %}
    WITH meta AS (
        SELECT
            registered_on AS _inserted_timestamp,
            file_name,
            {{ partition_function }} AS partition_key
        FROM
            TABLE(
                information_schema.external_table_files(
                    table_name => '{{ source( "bronze_streamline", model) }}'
                )
            ) A
    )
SELECT
    s.*,
    b.file_name,
    b._inserted_timestamp

    {% if balances %},
    r.block_timestamp :: TIMESTAMP AS block_timestamp
{% endif %}

{% if block_number %},
    COALESCE(
        TRY_TO_NUMBER(s.value :"BLOCK_NUMBER" :: STRING),
        TRY_TO_NUMBER(s.value :"block_number" :: STRING),
        TRY_TO_NUMBER(s.metadata :request :"data" :id :: STRING),
        TRY_TO_NUMBER(PARSE_JSON(
            s.metadata :request :"data"
        ) :id :: STRING)
    ) AS block_number
{% endif %}
FROM
    {{ source(
        "bronze_streamline",
        model
    ) }}
    s
    JOIN meta b
    ON b.file_name = metadata$filename
    AND b.partition_key = s.{{ partition_join_key }}

    {% if balances %}
        JOIN {{ ref('_block_ranges') }}
        r
        ON r.block_number = COALESCE(
            TRY_TO_NUMBER(s.value :"BLOCK_NUMBER" :: STRING),
            TRY_TO_NUMBER(s.value :"block_number" :: STRING)
        )
    {% endif %}
WHERE
    b.partition_key = s.{{ partition_join_key }}
    AND DATA :error IS NULL
    AND DATA IS NOT NULL
{% endmacro %}

{% macro v0_streamline_external_table_query_decoder(
        model
    ) %}
    WITH meta AS (
        SELECT
            job_created_time AS _inserted_timestamp,
            file_name,
            CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 6), '_', 1) AS INTEGER) AS _partition_by_block_number,
            TO_DATE(
                concat_ws('-', SPLIT_PART(file_name, '/', 3), SPLIT_PART(file_name, '/', 4), SPLIT_PART(file_name, '/', 5))
            ) AS _partition_by_created_date
        FROM
            TABLE(
                information_schema.external_table_file_registration_history(
                    start_time => DATEADD('day', -3, CURRENT_TIMESTAMP()),
                    table_name => '{{ source( "bronze_streamline", model) }}')
                ) A
            )
        SELECT
            block_number,
            id :: STRING AS id,
            DATA,
            metadata,
            b.file_name,
            _inserted_timestamp,
            s._partition_by_block_number AS _partition_by_block_number,
            s._partition_by_created_date AS _partition_by_created_date
        FROM
            {{ source(
                "bronze_streamline",
                model
            ) }}
            s
            JOIN meta b
            ON b.file_name = metadata$filename
            AND b._partition_by_block_number = s._partition_by_block_number
            AND b._partition_by_created_date = s._partition_by_created_date
        WHERE
            b._partition_by_block_number = s._partition_by_block_number
            AND b._partition_by_created_date = s._partition_by_created_date
            AND s._partition_by_created_date >= DATEADD('day', -2, CURRENT_TIMESTAMP())
            AND DATA :error IS NULL
            AND DATA IS NOT NULL
{% endmacro %}

{% macro v0_streamline_external_table_fr_query_decoder(
        model
    ) %}
    WITH meta AS (
        SELECT
            registered_on AS _inserted_timestamp,
            file_name,
            CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 6), '_', 1) AS INTEGER) AS _partition_by_block_number,
            TO_DATE(
                concat_ws('-', SPLIT_PART(file_name, '/', 3), SPLIT_PART(file_name, '/', 4), SPLIT_PART(file_name, '/', 5))
            ) AS _partition_by_created_date
        FROM
            TABLE(
                information_schema.external_table_files(
                    table_name => '{{ source( "bronze_streamline", model) }}'
                )
            ) A
    )
SELECT
    block_number,
    id :: STRING AS id,
    DATA,
    metadata,
    b.file_name,
    _inserted_timestamp,
    s._partition_by_block_number AS _partition_by_block_number,
    s._partition_by_created_date AS _partition_by_created_date
FROM
    {{ source(
        "bronze_streamline",
        model
    ) }}
    s
    JOIN meta b
    ON b.file_name = metadata$filename
    AND b._partition_by_block_number = s._partition_by_block_number
    AND b._partition_by_created_date = s._partition_by_created_date
WHERE
    b._partition_by_block_number = s._partition_by_block_number
    AND b._partition_by_created_date = s._partition_by_created_date
    AND DATA :error IS NULL
    AND DATA IS NOT NULL
{% endmacro %}