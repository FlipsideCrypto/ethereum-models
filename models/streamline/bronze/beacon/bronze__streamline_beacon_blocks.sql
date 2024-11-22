{{ config (
    materialized = 'view'
) }}

WITH meta AS (
    SELECT
        job_created_time AS _inserted_timestamp,
        file_name,
        CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER) AS partition_key
    FROM
        TABLE(
            information_schema.external_table_file_registration_history(
                start_time => DATEADD('hour', -4, SYSDATE()),
                table_name => '{{ source( "bronze_streamline", "beacon_blocks_v2") }}'
            )
        ) A
)
SELECT
    s.*,
    b.file_name,
    b._inserted_timestamp
    FROM
        {{ source( "bronze_streamline", "beacon_blocks_v2") }} s
    JOIN meta b
    ON b.file_name = metadata$filename
    AND b.partition_key = s.partition_key
    WHERE
        b.partition_key = s.partition_key
        AND DATA :error IS NULL
        AND DATA IS NOT NULL