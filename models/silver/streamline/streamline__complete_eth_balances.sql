{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"]
) }}

WITH meta AS (

    SELECT
        last_modified,
        file_name
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "eth_balances") }}'
            )
        ) A
    GROUP BY
        last_modified,
        file_name
)

{% if is_incremental() %},
max_date AS (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        ) max_INSERTED_TIMESTAMP
    FROM
        {{ this }}
)
{% endif %}
SELECT
    block_number,
    address,
    concat_ws(
        '-',
        block_number,
        address,
    ) AS id,
    last_modified AS _inserted_timestamp
FROM
    {{ source(
        "bronze_streamline",
        "eth_balances"
    ) }}
    JOIN meta b
    ON b.file_name = metadata$filename

{% if is_incremental() %}
WHERE
    b.last_modified > (
        SELECT
            max_INSERTED_TIMESTAMP
        FROM
            max_date
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
