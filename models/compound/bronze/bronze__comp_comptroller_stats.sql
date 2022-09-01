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
    LEAST(
        registered_on,
        last_modified
    ) >= (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }})
    ),
    partitions AS (
        SELECT
            DISTINCT TO_DATE(
                concat_ws('-', SPLIT_PART(file_name, '/', 3), SPLIT_PART(file_name, '/', 4), SPLIT_PART(file_name, '/', 5))
            ) AS _partition_by_modified_date
        FROM
            meta
    )
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
        ['block_number', 'contract_address', 'function_signature', 'function_input']
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
ON p._partition_by_modified_date = s._partition_by_modified_date
{% endif %}

WHERE
    _partition_by_function_signature in (
        '0x1d7b33d7',
        '0x6aa875b5',
        '0xf4a433c0')
    AND DATA :error IS NULL

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1