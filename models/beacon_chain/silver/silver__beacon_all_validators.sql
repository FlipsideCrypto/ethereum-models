{{ config(
    materialized = 'incremental',
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"]
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
                table_name => '{{ source( "bronze_streamline", "validators") }}'
            )
        )

{% if is_incremental() %}
WHERE
    LEAST(
        registered_on,
        last_modified
    ) >= (
        SELECT
            COALESCE(MAX(_inserted_timestamp), '1970-01-01' :: DATE) AS max_inserted_timestamp
        FROM
            {{ this }})
{% endif %}
)
SELECT
    VALUE AS read_value,
    _PARTITION_BY_BLOCK_ID,
    func_type,
    state_id,
    block_number,
    metadata,
    INDEX,
    m._inserted_timestamp :: TIMESTAMP AS _inserted_timestamp,
    DATA,
    {{ dbt_utils.surrogate_key(['block_number', 'func_type', 'index']) }} AS id
FROM
    {{ source(
        "bronze_streamline",
        "validators"
    ) }} s
JOIN meta m ON m.file_name = metadata$filename 
{% if is_incremental() %}
WHERE m._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 1
    FROM
        {{ this }} )
{% endif %}
QUALIFY(ROW_NUMBER() over (PARTITION BY id
    ORDER BY
        _inserted_timestamp DESC)) = 1