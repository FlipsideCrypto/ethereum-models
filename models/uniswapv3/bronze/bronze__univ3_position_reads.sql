{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['_inserted_timestamp::date'],
    merge_update_columns = ["id"]
) }}
-- 0x99fbab88
WITH meta AS (

    SELECT
        registered_on,
        file_name
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "reads") }}'
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
            DISTINCT SUBSTR(
                file_name,
                27,
                10
            ) AS partition_by_function_signature
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
    contract_address :: STRING AS contract_address,
    block_number AS block_number,
    function_input::integer as function_input,
    function_signature AS function_signature,
    DATA :result :: STRING AS read_output,
    m.registered_on AS _inserted_timestamp,
    {{ dbt_utils.surrogate_key(
        ['block_number', 'function_input']
    ) }} AS id
FROM
    {{ source(
        'bronze_streamline',
        'reads'
    ) }}
    s
    JOIN meta m
    ON m.file_name = metadata$filename

{% if is_incremental() %}
JOIN partitions p
ON p.partition_by_function_signature = s._partition_by_function_signature
{% endif %}
WHERE
    function_signature = '0x99fbab88'
    AND DATA :error IS NULL

{% if is_incremental() %}
AND m.registered_on > (
    SELECT
        max_INSERTED_TIMESTAMP
    FROM
        max_date
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
