{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(slot_number, -3)",
    merge_update_columns = ["id"]
) }}

WITH meta AS (

    SELECT
        last_modified,
        file_name
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "beacon_blocks") }}'
            )
        ) A
)

{% if is_incremental() %},
max_date AS (
    SELECT
        COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
    FROM
        {{ this }}),
        partitions AS (
            SELECT
                CAST(
                    SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER
                ) AS _partition_by_slot_id
            FROM
                meta
        )
    {% endif %}
    SELECT
        {{ dbt_utils.surrogate_key(
            ['slot_number']
        ) }} AS id,
        slot_number,
        last_modified AS _inserted_timestamp
    FROM
        {{ source(
            "bronze_streamline",
            "beacon_blocks"
        ) }}
        s
        JOIN meta b
        ON b.file_name = metadata$filename

{% if is_incremental() %}
JOIN partitions p
ON p._partition_by_slot_id = s._partition_by_slot_id
{% endif %}

{% if is_incremental() %}
WHERE
    b.last_modified > (
        SELECT
            max_INSERTED_TIMESTAMP
        FROM
            max_date
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
