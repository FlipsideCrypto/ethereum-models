{{ config(
    materialized = 'incremental',
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)"
) }}

WITH max_date AS (

    SELECT
        COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
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
                        table_name => '{{ source( "bronze_streamline", "beacon_validator_balances") }}'
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
    s.index,
    s.array_index,
    s.data :balance :: INTEGER / pow(
        10,
        9
    ) AS balance,
    _inserted_timestamp,
    {{ dbt_utils.surrogate_key(['block_number', 'index', 'array_index']) }} AS id
FROM
    {{ source(
        "bronze_streamline",
        "beacon_validator_balances"
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

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
