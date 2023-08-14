{{ config (
    materialized = 'table',
    tags = ['abis']
) }}

WITH abi_base AS (

    SELECT
        contract_address,
        DATA
    FROM
        {{ ref('silver__abis') }}
),
flat_abi AS (
    SELECT
        contract_address,
        DATA,
        VALUE :inputs AS inputs,
        VALUE :outputs AS outputs,
        VALUE :payable :: BOOLEAN AS payable,
        VALUE :stateMutability :: STRING AS stateMutability,
        VALUE :type :: STRING AS TYPE,
        VALUE :name :: STRING AS NAME
    FROM
        abi_base,
        LATERAL FLATTEN (
            input => DATA
        )
    WHERE
        TYPE = 'function'
),
flat_inputs AS (
    SELECT
        contract_address,
        inputs,
        NAME,
        ARRAY_AGG(
            VALUE :type :: STRING
        ) AS inputs_type
    FROM
        flat_abi,
        LATERAL FLATTEN (
            input => inputs
        )
    GROUP BY
        contract_address,
        inputs,
        NAME
),
flat_outputs AS (
    SELECT
        contract_address,
        outputs,
        NAME,
        ARRAY_AGG(
            VALUE :type :: STRING
        ) AS outputs_type
    FROM
        flat_abi,
        LATERAL FLATTEN (
            input => outputs
        )
    GROUP BY
        contract_address,
        outputs,
        NAME
),
apply_udfs AS (
    SELECT
        contract_address,
        NAME AS function_name,
        PARSE_JSON(
            OBJECT_CONSTRUCT(
                'inputs',
                inputs,
                'outputs',
                outputs,
                'name',
                NAME,
                'type',
                'function'
            ) :: STRING
        ) AS abi,
        utils.udf_evm_text_signature(abi) AS simple_function_name,
        utils.udf_keccak256(simple_function_name) AS function_signature,
        NAME,
        inputs,
        outputs,
        inputs_type,
        outputs_type
    FROM
        flat_inputs
        LEFT JOIN flat_outputs USING(
            contract_address,
            NAME
        )
)
SELECT
    contract_address,
    function_name,
    abi,
    simple_function_name,
    function_signature,
    LEFT(
        function_signature,
        10
    ) AS function_signature_prefix,
    NAME,
    inputs,
    outputs,
    inputs_type,
    outputs_type
FROM
    apply_udfs qualify ROW_NUMBER() over (
        PARTITION BY contract_address,
        NAME
        ORDER BY
            inputs_type DESC
    ) = 1
