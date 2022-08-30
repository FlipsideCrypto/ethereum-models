{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['_inserted_timestamp::date'],
    merge_update_columns = ["id"]
) }}

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
    _partition_by_function_signature in (
        '0x18160ddd',
        '0xf8f9da28',
        '0x182df0f5',
        '0xae9d70b0',
        '0x47bd3718',
        '0x8f840ddd')
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