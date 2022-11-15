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
    s.VALUE AS read_value,
    s._PARTITION_BY_BLOCK_ID,
    s.func_type,
    s.state_id,
    s.block_number,
    b.slot_timestamp,
    s.metadata,
    m._inserted_timestamp :: TIMESTAMP AS _inserted_timestamp,
    s.DATA,
    {{ dbt_utils.surrogate_key(
        ['s.block_number', 'func_type']
    ) }} AS id
FROM
    {{ source(
        "bronze_streamline",
        "committees"
    ) }}
    s
    
    JOIN meta m
      ON m.file_name = metadata$filename
    
    JOIN {{ ref('silver__beacon_blocks') }} b
      ON s.block_number = b.slot_number

WHERE
    s.DATA :message :: STRING IS NULL qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    m._inserted_timestamp DESC)) = 1
