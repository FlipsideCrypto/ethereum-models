{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['_inserted_timestamp::date', 'block_date::date'],
    merge_update_columns = ["id"]
) }}

WITH block_dates AS (

    SELECT
        block_date,
        block_number
    FROM
        {{ ref("_max_block_by_date") }}
),
meta AS (
    SELECT
        registered_on,
        file_name
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "eth_balances") }}'
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
    s.block_number :: INTEGER AS block_number,
    b.block_date :: DATE AS block_date,
    address,
    TRY_TO_NUMBER(
        PUBLIC.udf_hex_to_int(
            DATA :result :: STRING
        )
    ) AS balance,
    m.registered_on AS _inserted_timestamp,
    {{ dbt_utils.surrogate_key(
        ['s.block_number', 'address']
    ) }} AS id
FROM
    {{ source(
        'bronze_streamline',
        'eth_balances'
    ) }}
    s
    JOIN meta m
    ON m.file_name = metadata$filename
    LEFT JOIN block_dates b
    ON s.block_number = b.block_number

{% if is_incremental() %}
JOIN partitions p
ON p.partition_block_id = s._partition_by_block_id
{% endif %}
WHERE
    DATA :error IS NULL

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
