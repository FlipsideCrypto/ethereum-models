{{ config (
    materialized = 'table'
) }}

WITH proxy_base AS (

    SELECT
        C.created_contract_address AS contract_address,
        p.proxy_address,
        p.start_block
    FROM
        {{ ref('silver__created_contracts') }} C
        JOIN {{ ref('silver__proxies2') }}
        p
        ON C.created_contract_address = p.contract_address
),
contracts_base AS (
    SELECT
        created_contract_address AS contract_address,
        NULL AS proxy_address,
        block_number AS start_block
    FROM
        {{ ref('silver__created_contracts') }}
    WHERE
        created_contract_address NOT IN (
            SELECT
                proxy_address
            FROM
                proxy_base
        )
),
range_step1 AS (
    SELECT
        contract_address,
        proxy_address,
        start_block,
        FALSE AS contract_f
    FROM
        proxy_base
    UNION ALL
    SELECT
        contract_address,
        contract_address AS proxy_address,
        start_block,
        TRUE AS contract_f
    FROM
        contracts_base
),
range_step2 AS (
    SELECT
        *,
        IFNULL(LEAD(start_block) over (PARTITION BY contract_address
    ORDER BY
        start_block) -1, 1e18) AS next_block
    FROM
        range_step1
    WHERE
        contract_address = '0x2796317b0ff8538f253012862c06787adfb8ceb6'
),
range_step3 AS (
    SELECT
        f1.contract_address,
        f1.start_block,
        f1.next_block,
        f2.proxy_address,
        concat_ws(
            '-',
            f1.contract_address,
            f1.start_block,
            f1.next_block
        ) AS RANGE
    FROM
        range_step2 f1
        LEFT JOIN range_step2 f2
        ON f1.contract_address = f2.contract_address
        AND f1.next_block >= f2.start_block
),
abi_base AS (
    SELECT
        A.contract_address,
        A.data AS DATA,
        start_block,
        next_block,
        proxy_address,
        RANGE
    FROM
        {{ ref('silver__abis') }} A
        INNER JOIN range_step3 f
        ON A.contract_address = f.proxy_address
),
flat_abis AS (
    SELECT
        contract_address,
        proxy_address,
        start_block,
        next_block,
        RANGE,
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
        proxy_address,
        start_block,
        next_block,
        RANGE,
        inputs,
        anonymous,
        NAME,
        ARRAY_AGG(
            VALUE :type :: STRING
        ) AS event_type
    FROM
        flat_abis,
        LATERAL FLATTEN (
            input => inputs
        )
    GROUP BY
        contract_address,
        proxy_address,
        start_block,
        next_block,
        RANGE,
        inputs,
        anonymous,
        NAME
),
add_proxy_block AS (
    SELECT
        e.*,
        r.start_block AS proxy_start_block
    FROM
        event_types e
        JOIN range_step1 r
        ON e.proxy_address = r.proxy_address
),
FINAL AS (
    SELECT
        SPLIT(
            RANGE,
            '-'
        ) [0] :: STRING AS contract_address,
        SPLIT(
            RANGE,
            '-'
        ) [1] :: INT AS start_block,
        SPLIT(
            RANGE,
            '-'
        ) [2] :: INT AS end_block,
        RANGE,
        inputs,
        anonymous,
        NAME,
        OBJECT_CONSTRUCT(
            'anonymous',
            anonymous,
            'inputs',
            inputs,
            'name',
            NAME,
            'type',
            'event'
        ) AS complete_abi
    FROM
        add_proxy_block qualify(ROW_NUMBER() over(PARTITION BY RANGE, NAME, event_type
    ORDER BY
        proxy_start_block DESC)) = 1
)
SELECT
    contract_address,
    start_block,
    end_block,
    ARRAY_AGG(complete_abi) AS abi
FROM
    FINAL
GROUP BY
    contract_address,
    start_block,
    end_block
