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
all_contracts AS (
    SELECT
        contract_address,
        NAME AS function_name,
        inputs,
        outputs,
        inputs_type,
        outputs_type,
        _inserted_timestamp
    FROM
        flat_abi
        LEFT JOIN flat_inputs USING (
            contract_address,
            NAME
        )
        LEFT JOIN flat_outputs USING (
            contract_address,
            NAME
        )
),
apply_udfs AS (
    SELECT
        contract_address,
        function_name,
        PARSE_JSON(
            object_construct_keep_null(
                'inputs',
                inputs,
                'outputs',
                outputs,
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