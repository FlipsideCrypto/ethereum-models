{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"]
) }}

WITH meta AS (

    SELECT
        last_modified,
        file_name,
        CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER) AS _partition_by_block_id
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "contract_abis") }}'
            )
        ) A

{% if is_incremental() %}
WHERE
    last_modified >= (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }})
    )
{% else %}
)
{% endif %}
SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address', 'block_number']
    ) }} AS id,
    contract_address,
    block_number,
    last_modified AS _inserted_timestamp
FROM
    {{ source(
        "bronze_streamline",
        "contract_abis"
    ) }} t
    JOIN meta m
    ON m.file_name = metadata$filename
    AND m._partition_by_block_id = t._partition_by_block_id
WHERE
    m._partition_by_block_id = t._partition_by_block_id qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
