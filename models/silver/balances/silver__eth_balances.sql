{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['_inserted_timestamp::date', 'block_timestamp::date'],
    tags = ['balances'],
    merge_update_columns = ["id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH
block_dates AS (

    SELECT
        block_timestamp,
        block_number
    FROM
        {{ ref("silver__blocks") }}
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
    b.block_timestamp :: TIMESTAMP AS block_timestamp,
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
    join block_dates b
    on s.block_number = b.block_number


{% if is_incremental() %}
JOIN partitions p
ON p.partition_block_id = s._partition_by_block_id
{% endif %}
WHERE
    (DATA :error :code IS NULL
    OR DATA :error :code NOT IN (
        '-32000',
        '-32001',
        '-32002',
        '-32003',
        '-32004',
        '-32005',
        '-32006',
        '-32007',
        '-32008',
        '-32009',
        '-32010'
    ))

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
