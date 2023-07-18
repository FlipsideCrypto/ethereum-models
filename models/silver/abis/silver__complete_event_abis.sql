{{ config (
    materialized = 'table'
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
        VALUE :payable :: BOOLEAN AS payable,
        VALUE :stateMutability :: STRING AS stateMutability,
        VALUE :type :: STRING AS TYPE,
        VALUE :anonymous :: BOOLEAN AS anonymous,
        VALUE :name :: STRING AS NAME
    FROM
        abi_base,
        LATERAL FLATTEN (
            input => DATA
        )
    WHERE
        TYPE = 'event'
),
event_types AS (
    SELECT
        contract_address,
        inputs,
        anonymous,
        NAME,
        ARRAY_AGG(
            VALUE :type :: STRING
        ) AS event_type
    FROM
        flat_abi,
        LATERAL FLATTEN (
            input => inputs
        )
    GROUP BY
        contract_address,
        inputs,
        anonymous,
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
stacked AS (
    SELECT
        ea.contract_address,
        ea.inputs,
        ea.anonymous,
        ea.name,
        ea.event_type,
        pb.start_block,
        pb.contract_address AS base_contract_address,
        1 AS priority
    FROM
        event_types ea
        INNER JOIN proxy_base pb
        ON ea.contract_address = pb.proxy_address
    UNION ALL
    SELECT
        eab.contract_address,
        eab.inputs,
        eab.anonymous,
        eab.name,
        eab.event_type,
        pbb.created_block AS start_block,
        pbb.contract_address AS base_contract_address,
        2 AS priority
    FROM
        event_types eab
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
        eac.anonymous,
        eac.name,
        eac.event_type,
        0 AS start_block,
        eac.contract_address AS base_contract_address,
        3 AS priority
    FROM
        event_types eac
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
        NAME AS event_name,
        PARSE_JSON(
            OBJECT_CONSTRUCT(
                'anonymous',
                anonymous,
                'inputs',
                inputs,
                'name',
                NAME,
                'type',
                'event'
            ) :: STRING
        ) AS abi,
        start_block,
        utils.udf_evm_text_signature(abi) AS simple_event_name,
        utils.udf_keccak256(simple_event_name) AS event_signature,
        priority,
        NAME,
        inputs,
        event_type
    FROM
        stacked
),
FINAL AS (
    SELECT
        parent_contract_address,
        event_name,
        abi,
        start_block,
        simple_event_name,
        event_signature,
        NAME,
        inputs,
        event_type
    FROM
        apply_udfs qualify ROW_NUMBER() over (
            PARTITION BY parent_contract_address,
            NAME,
            event_type,
            start_block
            ORDER BY
                priority ASC
        ) = 1
)
SELECT
    parent_contract_address,
    event_name,
    abi,
    start_block,
    simple_event_name,
    event_signature,
    IFNULL(LEAD(start_block) over (PARTITION BY parent_contract_address, event_signature
ORDER BY
    start_block) -1, 1e18) AS end_block
FROM
    FINAL
