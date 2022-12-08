{{ config (
    materialized = "incremental",
    unique_key = "contract_address",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["contract_address"]
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
                table_name => '{{ source( "bronze_streamline", "contract_abis") }}'
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
    contract_address,
    block_number,
    DATA,
    _INSERTED_TIMESTAMP,
    metadata,
    VALUE
FROM
    {{ source(
        "bronze_streamline",
        "contract_abis"
    ) }}
    JOIN meta m
    ON m.file_name = metadata$filename
WHERE
    DATA :: STRING <> 'Contract source code not verified' qualify(ROW_NUMBER() over(PARTITION BY contract_address
ORDER BY
    block_number DESC, _INSERTED_TIMESTAMP DESC)) = 1
