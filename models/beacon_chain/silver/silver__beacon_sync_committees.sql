{{ config(
    materialized = 'incremental',
    unique_key = 'id',
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
                table_name => '{{ source( "bronze_streamline", "beacon_sync_committees") }}'
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
    s.block_number,
    s.state_id,
    s.DATA :validator_aggregates AS validator_aggregates,
    s.DATA :validators AS validators,
    _inserted_timestamp,
    {{ dbt_utils.surrogate_key(
        ['block_number']
    ) }} AS id
FROM
    {{ source(
        "bronze_streamline",
        "beacon_sync_committees"
    ) }}
    s
    JOIN meta m
    ON m.file_name = metadata$filename
WHERE
    s.data :message :: STRING IS NULL

{% if is_incremental() %}
AND m._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
