{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"]
) }}

WITH meta AS (

    SELECT
        registered_on,
        file_name
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "reads") }}'
            )
        ) A

{% if is_incremental() %}
WHERE
    registered_on >= (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }})
    ),
    partitions AS (
        SELECT
            DISTINCT to_date(concat_ws('-', split_part(file_name, '/', 3),split_part(file_name, '/', 4), split_part(file_name, '/', 5))) AS _partition_by_modified_date
        FROM
            meta
    ),
    max_date AS (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }})
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
{% endif %}

{% if is_incremental() %}
WHERE
    b.registered_on > (
        SELECT
            max_INSERTED_TIMESTAMP
        FROM
            max_date
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
