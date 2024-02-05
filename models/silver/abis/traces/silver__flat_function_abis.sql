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
        TYPE = 'function' qualify ROW_NUMBER() over (
            PARTITION BY contract_address,
            NAME
            ORDER BY
                _inserted_timestamp DESC
        ) = 1
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
fill_missing_input_names AS (
    SELECT
        contract_address,
        NAME,
        inputs_type,
        VALUE :internalType :: STRING AS internalType,
        VALUE :type :: STRING AS TYPE,
        CASE
            WHEN VALUE :name :: STRING = '' THEN CONCAT('input_', ROW_NUMBER() over (PARTITION BY contract_address, NAME
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
        contract_address,
        NAME,
        inputs_type
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
fill_missing_output_names AS (
    SELECT
        contract_address,
        NAME,
        outputs_type,
        VALUE :internalType :: STRING AS internalType,
        VALUE :type :: STRING AS TYPE,
        CASE
            WHEN VALUE :name :: STRING = '' THEN CONCAT('output_', ROW_NUMBER() over (PARTITION BY contract_address, NAME
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
        contract_address,
        NAME,
        outputs_type
),
all_contracts AS (
    SELECT
        A.contract_address,
        A.name AS function_name,
        i.inputs,
        o.outputs,
        i.inputs_type,
        o.outputs_type,
        A._inserted_timestamp
    FROM
        flat_abi A
        LEFT JOIN final_flat_inputs i
        ON A.contract_address = i.contract_address
        AND A.name = i.name
        LEFT JOIN final_flat_outputs o
        ON A.contract_address = o.contract_address
        AND A.name = o.name
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
        utils.udf_evm_text_signature(abi) AS simple_function_name,
        utils.udf_keccak256(simple_function_name) AS function_signature,
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
