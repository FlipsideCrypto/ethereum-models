{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    tags = ['streamline_view']
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
reads AS (
    SELECT
        *
    FROM
        {{ source(
            "bronze_streamline",
            "contract_abis"
        ) }}
        JOIN meta m
        ON m.file_name = metadata$filename
),
created_contracts AS (
    SELECT
        contract_address,
        block_number,
        _inserted_timestamp,
        UPPER(CONCAT(t.value :name :: STRING, '()')) AS text_signature
    FROM
        reads,
        LATERAL FLATTEN(
            input => DATA,
            MODE => 'array'
        ) t
    WHERE
        DATA != 'Contract source code not verified'
        AND t.value :type :: STRING = 'function'
        AND block_number IS NOT NULL
),
function_sigs AS (
    SELECT
        UPPER(text_signature) AS text_signature,
        bytes_signature
    FROM
        {{ ref('core__dim_function_signatures') }}
),
FINAL AS (
    SELECT
        contract_address,
        block_number,
        bytes_signature AS function_signature,
        NULL AS function_input,
        0 AS function_input_plug,
        _inserted_timestamp
    FROM
        created_contracts
        INNER JOIN function_sigs
        ON UPPER(
            created_contracts.text_signature
        ) = UPPER(
            function_sigs.text_signature
        )
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['block_number', 'contract_address', 'function_signature', 'function_input_plug']
    ) }} AS id,
    function_input,
    function_signature,
    block_number,
    contract_address,
    'eth_contract_meta_model' AS call_name,
    _inserted_timestamp
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
