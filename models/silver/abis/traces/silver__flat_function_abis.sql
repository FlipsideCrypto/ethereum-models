{{ config (
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'contract_address',
    cluster_by = '_inserted_timestamp::date',
    tags = ['abis'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY (contract_address)"
) }}

WITH abi_base AS (

    SELECT
        contract_address,
        DATA,
        _inserted_timestamp
    FROM
        {{ ref('silver__abis') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '24 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
flat_abi AS (
    SELECT
        contract_address,
        DATA,
        _inserted_timestamp,
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
udf_abis AS (
    SELECT
        *,
        PARSE_JSON(
            object_construct_keep_null(
                'inputs',
                IFNULL(
                    inputs,
                    []
                ),
                'outputs',
                IFNULL(
                    outputs,
                    []
                ),
                'name',
                NAME,
                'type',
                'function'
            ) :: STRING
        ) AS abi,
        utils.udf_evm_text_signature(abi) AS simple_function_name,
        utils.udf_keccak256(simple_function_name) AS function_signature
    FROM
        flat_abi qualify ROW_NUMBER() over (
            PARTITION BY contract_address,
            function_signature
            ORDER BY
                _inserted_timestamp DESC
        ) = 1
),
flat_inputs AS (
    SELECT
        contract_address,
        inputs,
        NAME,
        simple_function_name,
        function_signature,
        ARRAY_AGG(
            VALUE :type :: STRING
        ) AS inputs_type
    FROM
        udf_abis,
        LATERAL FLATTEN (
            input => inputs
        )
    GROUP BY
        ALL
),
fill_missing_input_names AS (
    SELECT
        contract_address,
        NAME,
        inputs_type,
        simple_function_name,
        function_signature,
        VALUE :internalType :: STRING AS internalType,
        VALUE :type :: STRING AS TYPE,
        CASE
            WHEN VALUE :name :: STRING = '' THEN CONCAT('input_', ROW_NUMBER() over (PARTITION BY contract_address, function_signature
            ORDER BY
                INDEX ASC) :: STRING)
                ELSE VALUE :name :: STRING
        END AS name_fixed,
        inputs,
        INDEX,
        VALUE :components AS components
    FROM
        flat_inputs,
        LATERAL FLATTEN (
            input => inputs
        )
),
final_flat_inputs AS (
    SELECT
        contract_address,
        NAME,
        inputs_type,
        simple_function_name,
        function_signature,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'internalType',
                internalType,
                'name',
                name_fixed,
                'type',
                TYPE,
                'components',
                components
            )
        ) within GROUP (
            ORDER BY
                INDEX
        ) AS inputs
    FROM
        fill_missing_input_names
    GROUP BY
        ALL
),
flat_outputs AS (
    SELECT
        contract_address,
        outputs,
        simple_function_name,
        function_signature,
        NAME,
        ARRAY_AGG(
            VALUE :type :: STRING
        ) AS outputs_type
    FROM
        udf_abis,
        LATERAL FLATTEN (
            input => outputs
        )
    GROUP BY
        ALL
),
fill_missing_output_names AS (
    SELECT
        contract_address,
        NAME,
        outputs_type,
        simple_function_name,
        function_signature,
        VALUE :internalType :: STRING AS internalType,
        VALUE :type :: STRING AS TYPE,
        CASE
            WHEN VALUE :name :: STRING = '' THEN CONCAT('output_', ROW_NUMBER() over (PARTITION BY contract_address, function_signature
            ORDER BY
                INDEX ASC) :: STRING)
                ELSE VALUE :name :: STRING
        END AS name_fixed,
        outputs,
        INDEX,
        VALUE :components AS components
    FROM
        flat_outputs,
        LATERAL FLATTEN (
            input => outputs
        )
),
final_flat_outputs AS (
    SELECT
        contract_address,
        NAME,
        outputs_type,
        simple_function_name,
        function_signature,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'internalType',
                internalType,
                'name',
                name_fixed,
                'type',
                TYPE,
                'components',
                components
            )
        ) within GROUP (
            ORDER BY
                INDEX
        ) AS outputs
    FROM
        fill_missing_output_names
    GROUP BY
        ALL
),
all_contracts AS (
    SELECT
        A.contract_address,
        A.name AS function_name,
        i.inputs,
        o.outputs,
        i.inputs_type,
        o.outputs_type,
        A._inserted_timestamp,
        A.function_signature,
        A.simple_function_name
    FROM
        udf_abis A
        LEFT JOIN final_flat_inputs i
        ON A.contract_address = i.contract_address
        AND A.function_signature = i.function_signature
        LEFT JOIN final_flat_outputs o
        ON A.contract_address = o.contract_address
        AND A.function_signature = o.function_signature
),
apply_udfs AS (
    SELECT
        contract_address,
        function_name,
        PARSE_JSON(
            object_construct_keep_null(
                'inputs',
                IFNULL(
                    inputs,
                    []
                ),
                'outputs',
                IFNULL(
                    outputs,
                    []
                ),
                'name',
                function_name,
                'type',
                'function'
            ) :: STRING
        ) AS abi,
        simple_function_name,
        function_signature,
        inputs,
        outputs,
        inputs_type,
        outputs_type,
        _inserted_timestamp
    FROM
        all_contracts
)
SELECT
    contract_address,
    function_name,
    abi,
    simple_function_name,
    function_signature,
    inputs,
    outputs,
    inputs_type,
    outputs_type,
    _inserted_timestamp
FROM
    apply_udfs
