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
                table_name => '{{ source( "bronze_streamline", "token_balances") }}'
            )
        ) A

{% if is_incremental() %}
WHERE
    registered_on >= (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }})
    ),
    partitions AS (
        SELECT
            DISTINCT TO_NUMBER(SPLIT_PART(file_name, '/', 3)) AS partition_block_id
        FROM
            meta
    ),
    max_date AS (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }})
        {% else %}
    )
{% endif %}
SELECT
    block_number,
    address,
    contract_address,
    {{ dbt_utils.surrogate_key(
        ['block_number', 'contract_address', 'address']
    ) }} AS id,
    m.registered_on AS _inserted_timestamp
FROM
    {{ source(
        "bronze_streamline",
        "token_balances"
    ) }} AS s
    JOIN meta m
    ON m.file_name = metadata$filename

{% if is_incremental() %}
JOIN partitions p
ON p.partition_block_id = s._partition_by_block_id
{% endif %}

{% if is_incremental() %}
WHERE
    m.registered_on > (
        SELECT
            max_INSERTED_TIMESTAMP
        FROM
            max_date
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
