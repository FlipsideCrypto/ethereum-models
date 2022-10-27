{{ config(
    materialized = 'incremental',
    unique_key = 'block_number',
    cluster_by = ['_PARTITION_BY_BLOCK_ID'],
    merge_update_columns = ["block_number"]
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
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }})
    )
{% else %}
)
{% endif %}
SELECT
    VALUE as read_value,
    _PARTITION_BY_BLOCK_ID,
    func_type,
    state_id,
    block_number,
    metadata,
    m._inserted_timestamp :: TIMESTAMP AS _inserted_timestamp,
    DATA
FROM
    {{ source(
        "bronze_streamline",
        "committees"
    ) }}
    s
    JOIN meta m
    ON m.file_name = metadata$filename 
