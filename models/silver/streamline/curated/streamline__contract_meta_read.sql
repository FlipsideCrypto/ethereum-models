{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    tags = ['streamline_view']
) }}

WITH created_contracts AS (

    SELECT
        contract_address,
        block_number AS created_block,
        UPPER(CONCAT(t.value :name :: STRING, '()')) AS text_signature
    FROM
        {{ ref('bronze__contract_abis') }},
        LATERAL FLATTEN(
            input => DATA,
            MODE => 'array'
        ) t
    WHERE
        DATA != 'Contract source code not verified'
        AND t.value :type :: STRING = 'function'
        AND block_number IS NOT NULL
),
contract_block_range AS (
    SELECT
        contract_address,
        block_number,
        text_signature,
        _inserted_timestamp
    FROM
        created_contracts
        JOIN {{ ref("silver__blocks") }}
        b
        ON b.block_number = created_contracts.created_block
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
        contract_block_range
        INNER JOIN function_sigs
        ON UPPER(
            contract_block_range.text_signature
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
