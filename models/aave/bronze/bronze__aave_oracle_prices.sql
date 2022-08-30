{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['_inserted_timestamp::date'],
    merge_update_columns = ["id"]
) }}
-- decimals(0x313ce567), name(0x06fdde03), and symbol(0x95d89b41)
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
            DISTINCT SPLIT_PART(SPLIT_PART(file_name, '/', 6), '_', 0) AS partition_by_function_signature
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
    function_signature AS function_signature,
    DATA :result :: STRING AS read_output,
    function_input,
    m.registered_on AS _inserted_timestamp,
    {{ dbt_utils.surrogate_key(
        ['block_number', 'contract_address', 'function_signature']
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
    _partition_by_function_signature = '0xb3596f07'
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