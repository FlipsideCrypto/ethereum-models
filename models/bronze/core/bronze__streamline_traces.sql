{{ config (
    materialized = 'view'
) }}

WITH meta AS (

    SELECT
        LEAST(
            last_modified,
            registered_on
        ) AS _inserted_timestamp,
        file_name,
        CAST(
            SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER
        ) AS _partition_by_block_number
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "traces") }}'
            )
        ) A
)
SELECT
    block_number,
    DATA,
    _inserted_timestamp,
    _partition_by_block_id AS _partition_by_block_number,
    {{ dbt_utils.surrogate_key(
        ['block_number']
    ) }} AS id
FROM
    {{ source(
        "bronze_streamline",
        "traces"
    ) }}
    s
    JOIN meta b
    ON b.file_name = metadata$filename
    and b._partition_by_block_number = s._partition_by_block_id
WHERE
    (
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
