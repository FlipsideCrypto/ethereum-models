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
                table_name => '{{ source( "bronze_streamline", "eth_v1_blocks") }}'
            )
        ) A
)
SELECT
    block_number,
    address,
    block_number AS id
FROM
    {{ source(
        "bronze_streamline",
        "eth_v1_blocks"
    ) }} AS s
    JOIN meta m
    ON m.file_name = metadata$filename
qualify(ROW_NUMBER() over (PARTITION BY id)) = 1
