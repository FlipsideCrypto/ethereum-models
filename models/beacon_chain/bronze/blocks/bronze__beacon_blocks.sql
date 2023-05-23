{{ config (
    materialized = 'view'
) }}

WITH meta AS (

    SELECT
        job_created_time AS _inserted_timestamp,
        file_name,
        CAST(
            SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER
        ) AS _partition_by_slot_id
    FROM
        TABLE(
            information_schema.external_table_file_registration_history(
                start_time => DATEADD('day', -3, CURRENT_TIMESTAMP()),
                table_name => '{{ source( "bronze_streamline", "beacon_blocks") }}')
            ) A
        )
    SELECT
        slot_number,
        b._inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['slot_number']
        ) }} AS id,
        s._partition_by_slot_id AS _partition_by_slot_id,
        s.data
    FROM
        {{ source(
            'bronze_streamline',
            'beacon_blocks'
        ) }}
        s
        JOIN meta b
        ON b.file_name = metadata$filename
        AND b._partition_by_slot_id = s._partition_by_slot_id
    WHERE
        b._partition_by_slot_id = s._partition_by_slot_id
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
            OR DATA NOT ILIKE '%not found%'
            OR DATA NOT ILIKE '%internal server error%'
        )
