{{ config (
    materialized = "incremental",
    unique_key = "_log_id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["_log_id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(_log_id)"
) }}

{% if is_incremental() %}
WITH meta AS (

    SELECT
        job_created_time,
        last_modified,
        file_name
    FROM
        TABLE(
            information_schema.external_table_file_registration_history(
                table_name => '{{ source( "bronze_streamline", "decoded_logs") }}',
                start_time => (
                    SELECT
                        MAX(_INSERTED_TIMESTAMP)
                    FROM
                        {{ this }}
                )
            )
        )
),
date_partitions AS (
    SELECT
        DISTINCT TO_DATE(
            concat_ws('-', SPLIT_PART(file_name, '/', 3), SPLIT_PART(file_name, '/', 4), SPLIT_PART(file_name, '/', 5))
        ) AS _partition_by_created_date
    FROM
        meta
),
block_partitions AS (
    SELECT
        DISTINCT CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 6), '_', 1) AS INTEGER) AS _partition_by_block_number
    FROM
        meta
)
{% else %}
    WITH meta AS (
        SELECT
            registered_on AS job_created_time,
            last_modified,
            file_name
        FROM
            TABLE(
                information_schema.external_table_files(
                    table_name => '{{ source( "bronze_streamline", "decoded_logs") }}'
                )
            ) A
    )
{% endif %}
SELECT
    block_number,
    id AS _log_id,
    job_created_time AS _inserted_timestamp
FROM
    {{ source(
        "bronze_streamline",
        "decoded_logs"
    ) }} AS s
    JOIN meta b
    ON b.file_name = metadata$filename

{% if is_incremental() %}
JOIN date_partitions p
ON p._partition_by_created_date = s._partition_by_created_date
JOIN block_partitions bp
ON bp._partition_by_block_number = s._partition_by_block_number
WHERE
    s._partition_by_created_date IN (
        CURRENT_DATE,
        CURRENT_DATE -1
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
