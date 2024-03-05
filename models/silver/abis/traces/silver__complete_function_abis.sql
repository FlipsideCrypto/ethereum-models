{{ config (
    materialized = 'incremental',
    unique_key = ['parent_contract_address','function_signature','start_block'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['abis']
) }}

WITH new_abis AS (

    SELECT
        DISTINCT contract_address
    FROM
        {{ ref('silver__flat_function_abis') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
proxies AS (
    SELECT
        p0.created_block,
        p0.proxy_created_block,
        p0.contract_address,
        p0.proxy_address,
        p0.start_block,
        p0._id,
        p0._inserted_timestamp
    FROM
        {{ ref('silver__proxies') }}
        p0
        JOIN new_abis na0
        ON p0.contract_address = na0.contract_address
    UNION
    SELECT
        p1.created_block,
        p1.proxy_created_block,
        p1.contract_address,
        p1.proxy_address,
        p1.start_block,
        p1._id,
        p1._inserted_timestamp
    FROM
        {{ ref('silver__proxies') }}
        p1
        JOIN new_abis na1
        ON p1.proxy_address = na1.contract_address
),
all_relevant_contracts AS (
    SELECT
        DISTINCT contract_address
    FROM
        proxies
    UNION
    SELECT
        DISTINCT proxy_address AS contract_address
    FROM
        proxies
    UNION
    SELECT
        contract_address
    FROM
        new_abis
),
flat_abis AS (
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
        JOIN all_relevant_contracts USING (contract_address)
),
base AS (
    SELECT
        ea.contract_address,
        function_name,
        abi,
        simple_function_name,
        function_signature,
        inputs,
        outputs,
        inputs_type,
        outputs_type,
        ea._inserted_timestamp,
        pb._inserted_timestamp AS proxy_inserted_timestamp,
        pb.start_block,
        pb.proxy_created_block,
        pb.contract_address AS base_contract_address,
        1 AS priority
    FROM
        flat_abis ea
        JOIN proxies pb
        ON ea.contract_address = pb.proxy_address
    UNION ALL
    SELECT
        eab.contract_address,
        function_name,
        abi,
        simple_function_name,
        function_signature,
        inputs,
        outputs,
        inputs_type,
        outputs_type,
        eab._inserted_timestamp,
        pbb._inserted_timestamp AS proxy_inserted_timestamp,
        pbb.created_block AS start_block,
        pbb.proxy_created_block,
        pbb.contract_address AS base_contract_address,
        2 AS priority
    FROM
        flat_abis eab
        JOIN (
            SELECT
                DISTINCT contract_address,
                created_block,
                proxy_created_block,
                _inserted_timestamp
            FROM
                proxies
        ) pbb
        ON eab.contract_address = pbb.contract_address
    UNION ALL
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
        _inserted_timestamp,
        NULL AS proxy_inserted_timestamp,
        0 AS start_block,
        NULL AS proxy_created_block,
        contract_address AS base_contract_address,
        3 AS priority
    FROM
        flat_abis eac
    WHERE
        contract_address NOT IN (
            SELECT
                DISTINCT contract_address
            FROM
                proxies
        )
),
new_records AS (
    SELECT
        base_contract_address AS parent_contract_address,
        function_name,
        abi,
        start_block,
        proxy_created_block,
        simple_function_name,
        function_signature,
        inputs,
        outputs,
        inputs_type,
        outputs_type,
        _inserted_timestamp,
        proxy_inserted_timestamp
    FROM
        base qualify ROW_NUMBER() over (
            PARTITION BY parent_contract_address,
            function_name,
            inputs_type,
            simple_function_name,
            start_block
            ORDER BY
                priority ASC,
                _inserted_timestamp DESC,
                proxy_created_block DESC nulls last,
                proxy_inserted_timestamp DESC nulls last
        ) = 1
)
SELECT
    parent_contract_address,
    function_name,
    abi,
    start_block,
    proxy_created_block,
    simple_function_name,
    function_signature,
    IFNULL(LEAD(start_block) over (PARTITION BY parent_contract_address, function_signature
ORDER BY
    start_block) -1, 1e18) AS end_block,
    _inserted_timestamp,
    proxy_inserted_timestamp,
    SYSDATE() AS _updated_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['parent_contract_address','function_signature','start_block']
    ) }} AS complete_event_abis_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    new_records qualify ROW_NUMBER() over (
        PARTITION BY parent_contract_address,
        function_name,
        function_signature,
        start_block
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
