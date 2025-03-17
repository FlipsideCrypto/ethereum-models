{% macro streamline_external_table_query_decoder(
        source_name,
        source_version
    ) %}
    
    {% if source_version != '' %}
        {% set source_version = '_' ~ source_version.lower() %}
    {% endif %}
    
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
                    table_name => '{{ source( "bronze_streamline", source_name ~ source_version) }}')
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
                source_name ~ source_version
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


{% macro streamline_external_table_query_decoder_fr(
        source_name,
        source_version
    ) %}
    
    {% if source_version != '' %}
        {% set source_version = '_' ~ source_version.lower() %}
    {% endif %} 
    
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
                    table_name => '{{ source( "bronze_streamline", source_name ~ source_version) }}'
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
        source_name ~ source_version
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
