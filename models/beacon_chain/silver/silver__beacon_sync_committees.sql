{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)"
) }}

WITH max_date AS (

    SELECT
        GREATEST(COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE), '2023-03-01' :: DATE) max_INSERTED_TIMESTAMP
    FROM
        {{ this }}),
        meta AS (
            SELECT
                registered_on,
                last_modified,
                LEAST(
                    last_modified,
                    registered_on
                ) AS _inserted_timestamp,
                file_name,
                CAST(
                    SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER
                ) AS _partition_by_block_number
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
            MAX(max_INSERTED_TIMESTAMP)
        FROM
            max_date
    )
{% endif %}
)
SELECT
    s.block_number,
    s.state_id,
    s.data :validator_aggregates AS validator_aggregates,
    s.data :validators AS validators,
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
    ON m._partition_by_block_number = s._partition_by_block_id
WHERE
    m._partition_by_block_number = s._partition_by_block_id

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
