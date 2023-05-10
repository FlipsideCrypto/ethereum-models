{{ config (
    materialized = "incremental",
    unique_key = "contract_address"
) }}

WITH abi_base AS (

    SELECT
        A.contract_address,
        proxy_address,
        start_block,
        A.data,
        A.abi_hash,
        A.abi_source,
        A.discord_username,
        A.bytecode,
        A._inserted_timestamp
    FROM
        {{ ref('silver__abis') }} A
        LEFT JOIN {{ ref('silver__proxies2') }}
        p USING(contract_address)

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) :: DATE
        FROM
            {{ this }}
    )
{% endif %}
),
flat_abi AS (
    SELECT
        contract_address,
        proxy_address,
        start_block,
        CASE
            WHEN proxy_address IS NULL THEN 1
            ELSE 2
        END AS priority,
        DATA,
        VALUE :inputs AS inputs,
        VALUE :payable :: BOOLEAN AS payable,
        VALUE :stateMutability :: STRING AS stateMutability,
        VALUE :type :: STRING AS TYPE,
        VALUE :anonymous :: BOOLEAN AS anonymous,
        VALUE :name :: STRING AS NAME,
        abi_source,
        bytecode,
        _inserted_timestamp
    FROM
        abi_base,
        LATERAL FLATTEN (
            input => DATA
        )
),
event_types AS (
    SELECT
        contract_address,
        proxy_address,
        priority,
        abi_source,
        bytecode,
        inputs,
        anonymous,
        NAME,
        ARRAY_AGG(
            VALUE :type :: STRING
        ) AS event_type,
        MAX(COALESCE(start_block, 0)) AS start_block,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        flat_abi,
        LATERAL FLATTEN (
            input => inputs
        )
    WHERE
        TYPE = 'event'
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8
),
contracts AS (
    SELECT
        contract_address AS parent_address,
        contract_address AS abi_address,
        priority,
        abi_source,
        bytecode,
        inputs,
        anonymous,
        NAME,
        event_type,
        start_block,
        _inserted_timestamp
    FROM
        event_types
    WHERE
        proxy_address IS NULL
        AND contract_address NOT IN (
            SELECT
                proxy_address
            FROM
                abi_base
            WHERE
                proxy_address IS NOT NULL
        )
),
proxies AS (
    SELECT
        p.contract_address AS parent_address,
        C.contract_address AS abi_address,
        priority,
        abi_source,
        bytecode,
        inputs,
        anonymous,
        NAME,
        event_type,
        C.start_block,
        C._inserted_timestamp
    FROM
        event_types C
        LEFT JOIN {{ ref('silver__proxies2') }}
        p
        ON C.contract_address = p.proxy_address
    WHERE
        C.proxy_address IS NULL
        AND C.contract_address IN (
            SELECT
                proxy_address
            FROM
                abi_base
            WHERE
                proxy_address IS NOT NULL
        )
),
proxies2 AS (
    SELECT
        contract_address AS parent_address,
        proxy_address AS abi_address,
        priority,
        abi_source,
        bytecode,
        inputs,
        anonymous,
        NAME,
        event_type,
        start_block,
        _inserted_timestamp
    FROM
        event_types
    WHERE
        proxy_address IS NOT NULL
),
all_cases AS (
    SELECT
        parent_address,
        abi_address,
        priority,
        abi_source,
        bytecode,
        inputs,
        anonymous,
        NAME,
        event_type,
        start_block,
        _inserted_timestamp
    FROM
        contracts
    UNION ALL
    SELECT
        parent_address,
        abi_address,
        priority,
        abi_source,
        bytecode,
        inputs,
        anonymous,
        NAME,
        event_type,
        start_block,
        _inserted_timestamp
    FROM
        proxies
    UNION ALL
    SELECT
        parent_address,
        abi_address,
        priority,
        abi_source,
        bytecode,
        inputs,
        anonymous,
        NAME,
        event_type,
        start_block,
        _inserted_timestamp
    FROM
        proxies2
),
abi_priority AS (
    SELECT
        parent_address,
        abi_address,
        abi_source,
        bytecode,
        inputs,
        anonymous,
        NAME,
        event_type,
        _inserted_timestamp
    FROM
        all_cases qualify(ROW_NUMBER() over(PARTITION BY parent_address, NAME, event_type :: STRING
    ORDER BY
        priority ASC, start_block DESC)) = 1
),
FINAL AS (
    SELECT
        parent_address,
        abi_address,
        OBJECT_CONSTRUCT(
            'anonymous',
            anonymous,
            'inputs',
            inputs,
            'name',
            NAME,
            'type',
            'event'
        ) AS complete_abi,
        abi_source,
        bytecode,
        _inserted_timestamp
    FROM
        abi_priority
)
SELECT
    parent_address AS contract_address,
    ARRAY_AGG(
        DISTINCT abi_address
    ) AS abi_addresses,
    ARRAY_AGG(
        complete_abi
    ) AS abi,
    ARRAY_AGG(
        DISTINCT abi_source
    ) AS abi_source,
    ARRAY_AGG(
        DISTINCT bytecode
    ) AS bytecode,
    MAX(_inserted_timestamp) AS _inserted_timestamp
FROM
    FINAL
GROUP BY
    1
