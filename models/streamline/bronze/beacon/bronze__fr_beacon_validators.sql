{{ config (
    materialized = 'view'
) }}

WITH meta AS (

    SELECT
        registered_on AS _inserted_timestamp,
        file_name,
        CAST(
            SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER
        ) AS _partition_by_block_id
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "beacon_validators") }}'
            )
        ) A
)
SELECT
    block_number,
    state_id,
    INDEX,
    array_index,
    b._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_number', 'index', 'array_index']) }} AS id,
    s._partition_by_block_id AS _partition_by_block_id,
    DATA
FROM
    {{ source(
        'bronze_streamline',
        'beacon_validators'
    ) }}
    s
    JOIN meta b
    ON b.file_name = metadata$filename
    AND b._partition_by_block_id = s._partition_by_block_id
WHERE
    b._partition_by_block_id = s._partition_by_block_id
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
    AND DATA NOT ILIKE '%not found%'
    AND DATA NOT ILIKE '%internal server error%'
