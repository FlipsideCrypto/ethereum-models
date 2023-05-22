{{ config (
    materialized = 'view'
) }}

WITH meta AS (

    SELECT
        job_created_time AS _inserted_timestamp,
        file_name,
        TO_DATE(
            concat_ws('-', SPLIT_PART(file_name, '/', 3), SPLIT_PART(file_name, '/', 4), SPLIT_PART(file_name, '/', 5))
        ) AS _partition_by_modified_date
    FROM
        TABLE(
            information_schema.external_table_file_registration_history(
                start_time => DATEADD('day', -3, CURRENT_TIMESTAMP()),
                table_name => '{{ source( "bronze_streamline", "reads") }}')
            ) A
        )
    SELECT
        {{ dbt_utils.generate_surrogate_key(
            ['contract_address', 'function_signature', 'call_name', 'function_input', 'block_number']
        ) }} AS id,
        contract_address,
        function_signature,
        call_name,
        NULLIF(
            function_input,
            'None'
        ) AS function_input,
        block_number,
        DATA,
        _inserted_timestamp,
        s._partition_by_modified_date AS _partition_by_modified_date
    FROM
        {{ source(
            "bronze_streamline",
            "reads"
        ) }}
        s
        JOIN meta b
        ON b.file_name = metadata$filename
        AND b._partition_by_modified_date = s._partition_by_modified_date
    WHERE
        b._partition_by_modified_date = s._partition_by_modified_date
        AND s._partition_by_modified_date >= DATEADD('day', -2, CURRENT_TIMESTAMP())
        AND (
            DATA :error :code IS NULL
            OR DATA :error :code NOT IN (
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
