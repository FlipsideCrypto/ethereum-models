{{ config (
    materialized = "incremental",
    unique_key = "ID",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["ID"]
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
{% endif %},
base_data AS (
    SELECT
        contract_address,
        block_number,
        DATA,
        _INSERTED_TIMESTAMP,
        metadata,
        VALUE,
        {{ dbt_utils.surrogate_key(
            ['contract_address', 'block_number']
        ) }} AS id
    FROM
        {{ source(
            "bronze_streamline",
            "contract_abis"
        ) }}
        JOIN meta m
        ON m.file_name = metadata$filename
    WHERE
        DATA :: STRING <> 'Contract source code not verified' qualify(ROW_NUMBER() over(PARTITION BY id
    ORDER BY
        _INSERTED_TIMESTAMP DESC)) = 1
)

{% if is_incremental() %},
update_records AS (
    SELECT
        contract_address,
        block_number,
        DATA,
        _INSERTED_TIMESTAMP,
        metadata,
        VALUE,
        id
    FROM
        {{ this }}
    WHERE
        contract_address IN (
            SELECT
                DISTINCT contract_address
            FROM
                base_data
        )
),
all_records AS (
    SELECT
        contract_address,
        block_number,
        DATA,
        _INSERTED_TIMESTAMP,
        metadata,
        VALUE,
        id
    FROM
        update_records
    UNION ALL
    SELECT
        contract_address,
        block_number,
        DATA,
        _INSERTED_TIMESTAMP,
        metadata,
        VALUE,
        id
    FROM
        base_data
)
{% endif %},
FINAL AS (
    SELECT
        contract_address,
        block_number,
        DATA,
        _INSERTED_TIMESTAMP,
        metadata,
        VALUE,
        id,
        ROW_NUMBER() over (
            PARTITION BY contract_address
            ORDER BY
                block_number DESC
        ) AS row_no_desc,
        ROW_NUMBER() over (
            PARTITION BY contract_address
            ORDER BY
                block_number ASC
        ) AS row_no_asc
    FROM

{% if is_incremental() %}
all_records qualify(ROW_NUMBER() over(PARTITION BY id
ORDER BY
    _INSERTED_TIMESTAMP DESC)) = 1
{% else %}
    base_data
{% endif %}
)
SELECT
    f1.contract_address,
    f1.block_number,
    f1.data,
    f1._INSERTED_TIMESTAMP,
    f1.metadata,
    f1.value,
    f1.id,
    COALESCE(
        f2.block_number - 1,
        10000000000000
    ) AS end_block,
    CASE
        WHEN f3.block_number IS NULL THEN 0
        ELSE f1.block_number
    END AS start_block
FROM
    FINAL f1
    LEFT JOIN FINAL f2
    ON f1.row_no_desc - 1 = f2.row_no_desc
    AND f1.contract_address = f2.contract_address
    LEFT JOIN FINAL f3
    ON f1.row_no_desc = f3.row_no_desc - 1
    AND f1.contract_address = f3.contract_address
