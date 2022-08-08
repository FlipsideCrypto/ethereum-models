{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['block_date'],
    tags = ['balances']
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
    b.block_date :: DATE AS block_date,
    address,
    contract_address,
    TRY_TO_NUMBER(
        PUBLIC.udf_hex_to_int(
            VALUE :data :result :: STRING
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
    LEFT JOIN block_dates b
    ON s.block_number = b.block_number

{% if is_incremental() %}
JOIN partitions p
ON p.partition_block_id = s._partition_by_block_id
{% endif %}
WHERE
    address IN (
        '0xf76e2d2bba0292cf88f71934aff52ea54baa64d9',
        LOWER('0xf82695449c70A7f136f160EdF7bca6781C6746Ae'),
        LOWER('0x34D6B21D7B773225A102b382815e00Ad876E23C2'),
        LOWER('0x581BEf12967f06f2eBfcabb7504fA61f0326CD9A'),
        LOWER('0xccC3330A95D9F83e2aFAA68911BB52468E2B26FE'),
        LOWER('0x64D7DB42c8b66fA5F43aeB36D3D4bEF0F8207A56'),
        LOWER('0x6fe5b79de59511ca15ae1a56b1ad0f74cd5024cb'),
        LOWER('0x8De9C5A032463C561423387a9648c5C7BCC5BC90'),
        LOWER('0x397FF1542f962076d0BFE58eA045FfA2d347ACa0')
    )

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
