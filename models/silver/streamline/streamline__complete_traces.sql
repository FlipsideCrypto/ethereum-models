{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)"
) }}

WITH meta AS (

    SELECT
        CAST(
            SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER
        ) AS _partition_by_block_number
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "traces") }}'
            )
        ) A
    {% if is_incremental() %}
    where
        last_modified >= (
            select
                max(max_INSERTED_TIMESTAMP)
            from
                (
                    SELECT
                        COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
                    FROM
                        {{ this }}
                )
        )
    {% endif %}

)

SELECT
    {{ dbt_utils.surrogate_key(
        ['block_number', 'tx_id']
    ) }} AS id,
    *,
    (
        select
            max(max_INSERTED_TIMESTAMP)
        from
            (
                SELECT
                    COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
                FROM
                    {{ this }}
            )
    ) as _inserted_timestamp
FROM
    {{ source(
        "bronze_streamline",
        "traces"
    ) }} t
    JOIN meta b ON b._partition_by_block_number = t._partition_by_block_id
WHERE
    b._partition_by_block_number = t._partition_by_block_id
