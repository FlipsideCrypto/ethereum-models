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
                table_name => '{{ source( "bronze_streamline", "committees") }}'
            )
        ) A

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
    s.value AS read_value,
    s._PARTITION_BY_BLOCK_ID,
    s.func_type,
    s.state_id,
    s.block_number,
    s.metadata,
    m._inserted_timestamp :: TIMESTAMP AS _inserted_timestamp,
    s.data,
    {{ dbt_utils.surrogate_key(['s.block_number', 'func_type']) }} AS id
FROM
    {{ source(
        "bronze_streamline",
        "committees"
    ) }}
    s
JOIN meta m ON m.file_name = metadata$filename
WHERE s.data :message :: STRING IS NULL
-- {% if is_incremental() %}
-- AND m._inserted_timestamp IN (
--     CURRENT_DATE,
--     CURRENT_DATE -1
-- )
-- {% endif %}
QUALIFY (ROW_NUMBER() over (PARTITION BY id
    ORDER BY
        m._inserted_timestamp DESC)) = 1