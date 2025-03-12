{% macro streamline_external_table_query(
        source_name,
        source_version,
        partition_function,
        balances,
        block_number,
        uses_receipts_by_hash
    ) %}

    {% if source_version != '' %}
        {% set source_version = '_' ~ source_version.lower() %}
    {% endif %}

    WITH meta AS (
        SELECT
            job_created_time AS _inserted_timestamp,
            file_name,
            {{ partition_function }} AS partition_key
        FROM
            TABLE(
                information_schema.external_table_file_registration_history(
                    start_time => DATEADD('day', -3, CURRENT_TIMESTAMP()),
                    table_name => '{{ source( "bronze_streamline", source_name ~ source_version) }}')
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
                s.value :"BLOCK_NUMBER" :: STRING,
                s.metadata :request :"data" :id :: STRING,
                PARSE_JSON(
                    s.metadata :request :"data"
                ) :id :: STRING
            ) :: INT AS block_number
        {% endif %}
        {% if uses_receipts_by_hash %},
            s.value :"TX_HASH" :: STRING AS tx_hash
        {% endif %}
        FROM
            {{ source(
                "bronze_streamline",
                source_name ~ source_version
            ) }}
            s
            JOIN meta b
            ON b.file_name = metadata$filename
            AND b.partition_key = s.partition_key

            {% if balances %}
            JOIN {{ ref('_block_ranges') }}
            r
            ON r.block_number = COALESCE(
                s.value :"BLOCK_NUMBER" :: INT,
                s.value :"block_number" :: INT
            )
        {% endif %}
        WHERE
            b.partition_key = s.partition_key
            AND DATA :error :code IS NULL
            AND DATA IS NOT NULL
{% endmacro %}

{% macro streamline_external_table_query_fr(
        source_name,
        source_version,
        partition_function,
        partition_join_key,
        balances,
        block_number,
        uses_receipts_by_hash
    ) %}

    {% if source_version != '' %}
        {% set source_version = '_' ~ source_version.lower() %}
    {% endif %}
    
    WITH meta AS (
        SELECT
            registered_on AS _inserted_timestamp,
            file_name,
            {{ partition_function }} AS partition_key
        FROM
            TABLE(
                information_schema.external_table_files(
                    table_name => '{{ source( "bronze_streamline", source_name ~ source_version) }}'
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
        s.value :"BLOCK_NUMBER" :: STRING,
        s.value :"block_number" :: STRING,
        s.metadata :request :"data" :id :: STRING,
        PARSE_JSON(
            s.metadata :request :"data"
        ) :id :: STRING
    ) :: INT AS block_number
{% endif %}
{% if uses_receipts_by_hash %},
    s.value :"TX_HASH" :: STRING AS tx_hash
{% endif %}
FROM
    {{ source(
        "bronze_streamline",
        source_name ~ source_version
    ) }}
    s
    JOIN meta b
    ON b.file_name = metadata$filename
    AND b.partition_key = s.{{ partition_join_key }}

    {% if balances %}
        JOIN {{ ref('_block_ranges') }}
        r
        ON r.block_number = COALESCE(
            s.value :"BLOCK_NUMBER" :: INT,
            s.value :"block_number" :: INT
        )
    {% endif %}
WHERE
    b.partition_key = s.{{ partition_join_key }}
    AND DATA :error :code IS NULL
    AND DATA IS NOT NULL
{% endmacro %}
