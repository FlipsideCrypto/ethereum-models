{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"]
) }}

WITH meta AS (

    SELECT
        registered_on,
        last_modified,
        file_name
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "reads") }}'
            )
        ) A

{% if is_incremental() %}
WHERE
    LEAST(
        registered_on,
        last_modified
    ) >= (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }})
    ),
    partitions AS (
        SELECT
            DISTINCT TO_DATE(
                concat_ws('-', SPLIT_PART(file_name, '/', 3), SPLIT_PART(file_name, '/', 4), SPLIT_PART(file_name, '/', 5))
            ) AS _partition_by_modified_date
        FROM
            meta
    )
{% else %}
)
{% endif %}
SELECT
    {{ dbt_utils.surrogate_key(
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
    registered_on AS _inserted_timestamp
FROM
    {{ source(
        "bronze_streamline",
        "reads"
    ) }} AS s
    JOIN meta b
    ON b.file_name = metadata$filename

{% if is_incremental() %}
JOIN partitions p
ON p._partition_by_modified_date = s._partition_by_modified_date
where s._partition_by_modified_date in (current_date, current_date-1)
and (data:error:code is null or data:error:code not like '-32%')
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
