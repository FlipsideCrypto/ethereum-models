{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['_inserted_timestamp::date', 'function_signature'],
    merge_update_columns = ["id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH meta AS (

    SELECT
        registered_on,
        last_modified,
        LEAST(
            last_modified,
            registered_on
        ) AS _inserted_timestamp,
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
    contract_address :: STRING AS contract_address,
    block_number :: INTEGER AS block_number,
    function_signature :: STRING AS function_signature,
    call_name :: STRING AS call_name,
    DATA :result :: STRING AS read_output,
    function_input :: STRING AS function_input,
    regexp_substr_all(SUBSTR(read_output, 3, len(read_output)), '.{64}') AS segmented_data,
    m._inserted_timestamp :: TIMESTAMP AS _inserted_timestamp,
    {{ dbt_utils.surrogate_key(
        ['block_number', 'contract_address', 'function_signature', 'function_input']
    ) }} AS id
FROM
    {{ source(
        "bronze_streamline",
        "reads"
    ) }} s
    JOIN meta m
    ON m.file_name = metadata$filename

{% if is_incremental() %}
JOIN partitions p
ON p._partition_by_modified_date = s._partition_by_modified_date
{% endif %}
WHERE
    DATA :error IS NULL qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
