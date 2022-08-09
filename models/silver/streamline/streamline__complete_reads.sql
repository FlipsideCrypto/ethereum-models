{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"]
) }}

WITH meta AS (

    SELECT
        last_modified,
        file_name
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "reads") }}'
            )
        ) A
    GROUP BY
        last_modified,
        file_name
)

{% if is_incremental() %},
max_date AS (
    SELECT
        COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
    FROM
        {{ this }})
    {% endif %}
    SELECT
        id,
        contract_address,
        function_signature,
        call_name,
        function_input,
        block_number
        last_modified AS _inserted_timestamp
    FROM
        {{ source(
            "bronze_streamline",
            "reads"
        ) }}
        JOIN meta b
        ON b.file_name = metadata$filename

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
