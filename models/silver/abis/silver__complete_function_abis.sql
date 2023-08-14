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
proxy_base AS (
    SELECT
        C.created_contract_address AS contract_address,
        p.proxy_address,
        p.start_block,
        C.block_number AS created_block
    FROM
        {{ ref('silver__created_contracts') }} C
        INNER JOIN {{ ref('silver__proxies') }}
        p
        ON C.created_contract_address = p.contract_address
        AND p.proxy_address <> '0x0000000000000000000000000000000000000000'
),
all_contracts AS (
    SELECT
        contract_address,
        NAME AS function_name,
        inputs,
        outputs,
        inputs_type,
        outputs_type
    FROM
        flat_inputs
        LEFT JOIN flat_outputs USING (
            contract_address,
            NAME
        )
),
stacked AS (
    SELECT
        ea.contract_address,
        ea.inputs,
        ea.outputs,
        ea.function_name,
        ea.inputs_type,
        ea.outputs_type,
        pb.start_block,
        pb.contract_address AS base_contract_address,
        1 AS priority
    FROM
        all_contracts ea
        INNER JOIN proxy_base pb
        ON ea.contract_address = pb.proxy_address
    UNION ALL
    SELECT
        eab.contract_address,
        eab.inputs,
        eab.outputs,
        eab.function_name,
        eab.inputs_type,
        eab.outputs_type,
        pbb.created_block AS start_block,
        pbb.contract_address AS base_contract_address,
        2 AS priority
    FROM
        all_contracts eab
        INNER JOIN (
            SELECT
                DISTINCT contract_address,
                created_block
            FROM
                proxy_base
        ) pbb
        ON eab.contract_address = pbb.contract_address
    UNION ALL
    SELECT
        eac.contract_address,
        eac.inputs,
        eac.outputs,
        eac.function_name,
        eac.inputs_type,
        eac.outputs_type,
        0 AS start_block,
        eac.contract_address AS base_contract_address,
        3 AS priority
    FROM
        all_contracts eac
    WHERE
        contract_address NOT IN (
            SELECT
                DISTINCT contract_address
            FROM
                proxy_base
        )
),
apply_udfs AS (
    SELECT
        contract_address AS source_contract_address,
        base_contract_address AS parent_contract_address,
        function_name,
        PARSE_JSON(
            OBJECT_CONSTRUCT(
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
        start_block,
        utils.udf_evm_text_signature(abi) AS simple_function_name,
        utils.udf_keccak256(simple_function_name) AS function_signature,
        priority,
        inputs,
        outputs,
        inputs_type,
        outputs_type
    FROM
        stacked
),
FINAL AS (
    SELECT
        parent_contract_address,
        function_name,
        abi,
        start_block,
        simple_function_name,
        function_signature,
        inputs,
        outputs,
        inputs_type,
        outputs_type
    FROM
        apply_udfs qualify ROW_NUMBER() over (
            PARTITION BY parent_contract_address,
            function_name,
            inputs_type,
            start_block
            ORDER BY
                priority ASC
        ) = 1
)
SELECT
    parent_contract_address,
    function_name,
    abi,
    start_block,
    simple_function_name,
    function_signature,
    LEFT(
        function_signature,
        10
    ) AS function_signature_prefix,
    IFNULL(LEAD(start_block) over (PARTITION BY parent_contract_address, function_signature
ORDER BY
    start_block) -1, 1e18) AS end_block
FROM
    FINAL
