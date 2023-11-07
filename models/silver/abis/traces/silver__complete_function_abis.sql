{{ config (
    materialized = 'incremental',
    unique_key = ['parent_contract_address','function_signature','start_block'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['abis']
) }}

WITH flat_abis AS (

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
        {{ ref('silver__flat_function_abis') }}

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
base AS (
    SELECT
        ea.contract_address,
        inputs,
        outputs,
        function_name,
        inputs_type,
        outputs_type,
        start_block,
        pb.contract_address AS base_contract_address,
        1 AS priority,
        ea._inserted_timestamp,
        abi,
        simple_function_name,
        function_signature
    FROM
        flat_abis ea
        JOIN {{ ref('silver__proxies') }}
        pb
        ON ea.contract_address = pb.proxy_address
    UNION ALL
    SELECT
        contract_address,
        inputs,
        outputs,
        function_name,
        inputs_type,
        outputs_type,
        created_block AS start_block,
        contract_address AS base_contract_address,
        2 AS priority,
        _inserted_timestamp,
        abi,
        simple_function_name,
        function_signature
    FROM
        flat_abis eab
        JOIN (
            SELECT
                DISTINCT contract_address,
                created_block
            FROM
                {{ ref('silver__proxies') }}
        ) pbb USING (contract_address)
    UNION ALL
    SELECT
        contract_address,
        inputs,
        outputs,
        function_name,
        inputs_type,
        outputs_type,
        0 AS start_block,
        contract_address AS base_contract_address,
        3 AS priority,
        _inserted_timestamp,
        abi,
        simple_function_name,
        function_signature
    FROM
        flat_abis
    WHERE
        contract_address NOT IN (
            SELECT
                DISTINCT contract_address
            FROM
                {{ ref('silver__proxies') }}
        )
),
new_records AS (
    SELECT
        base_contract_address AS parent_contract_address,
        function_name,
        abi,
        start_block,
        simple_function_name,
        function_signature,
        inputs,
        outputs,
        inputs_type,
        outputs_type,
        _inserted_timestamp
    FROM
        base qualify ROW_NUMBER() over (
            PARTITION BY parent_contract_address,
            function_name,
            inputs_type,
            start_block
            ORDER BY
                priority ASC
        ) = 1
)

{% if is_incremental() %},
heal_records AS (
    SELECT
        parent_contract_address,
        function_name,
        abi,
        start_block,
        simple_function_name,
        function_signature,
        _inserted_timestamp
    FROM
        {{ this }}
    WHERE
        parent_contract_address IN (
            SELECT
                DISTINCT parent_contract_address
            FROM
                new_records
        )
)
{% endif %},
FINAL AS (
    SELECT
        parent_contract_address,
        function_name,
        abi,
        start_block,
        simple_function_name,
        function_signature,
        _inserted_timestamp
    FROM
        new_records

{% if is_incremental() %}
UNION ALL
SELECT
    parent_contract_address,
    function_name,
    abi,
    start_block,
    simple_function_name,
    function_signature,
    _inserted_timestamp
FROM
    heal_records
{% endif %}
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
    start_block) -1, 1e18) AS end_block,
    _inserted_timestamp,
    SYSDATE() AS _updated_timestamp
FROM
    FINAL qualify ROW_NUMBER() over (
        PARTITION BY parent_contract_address,
        function_name,
        function_signature,
        start_block
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
