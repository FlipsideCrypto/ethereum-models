{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)"
) }}

WITH max_date AS (

    SELECT
        COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
    FROM
        {{ this }}),
        meta AS (
            SELECT
                registered_on,
                last_modified,
                file_name
            FROM
                TABLE(
                    information_schema.external_table_files(
                        table_name => '{{ source( "bronze_streamline", "traces") }}'
                    )
                ) A

{% if is_incremental() %}
WHERE
    LEAST(
        registered_on,
        last_modified
    ) >= (
        SELECT
            MAX(max_INSERTED_TIMESTAMP)
        FROM
            max_date
    )
)
{% else %}
)
{% endif %}
SELECT
    {{ dbt_utils.surrogate_key(
        ['block_number', 'tx_id']
    ) }} AS id,*
FROM
    (
        SELECT
            block_number,
            SPLIT_PART(
                DATA :id,
                '-',
                2
            ) AS tx_id,
            last_modified AS _inserted_timestamp
        FROM
            {{ source(
                "bronze_streamline",
                "traces"
            ) }}
            JOIN meta b
            ON b.file_name = metadata$filename
    ) qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
