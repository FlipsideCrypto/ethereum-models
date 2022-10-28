{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['_inserted_timestamp::date', 'block_timestamp::date'],
    tags = ['balances'],
    merge_update_columns = ["id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH block_dates AS (

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
    s.block_number :: INTEGER AS block_number,
    b.block_timestamp :: TIMESTAMP AS block_timestamp,
    address,
    contract_address,
    TRY_TO_NUMBER(
        PUBLIC.udf_hex_to_int(
            DATA :result :: STRING
        )
    ) AS balance,
    m.registered_on AS _inserted_timestamp,
    {{ dbt_utils.surrogate_key(
        ['s.block_number', 'contract_address', 'address']
    ) }} AS id
FROM
    {{ source(
        'bronze_streamline',
        'token_balances'
    ) }}
    s
    JOIN meta m
    ON m.file_name = metadata$filename
    JOIN block_dates b
    ON s.block_number = b.block_number

{% if is_incremental() %}
JOIN partitions p
ON p.partition_block_id = s._partition_by_block_id
{% endif %}
WHERE
    DATA :error IS NULL
    AND DATA :result :: STRING <> '0x'

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
