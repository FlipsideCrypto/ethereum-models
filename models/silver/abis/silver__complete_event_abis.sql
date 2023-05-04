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
        LEFT JOIN {{ ref('silver__proxies') }}
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
        SHA2_HEX(
            CONCAT(
                contract_address,
                NAME,
                event_type :: STRING
            )
        ) AS _unique_key,
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
abi_priority AS (
    SELECT
        contract_address,
        proxy_address,
        priority,
        abi_source,
        bytecode,
        inputs,
        anonymous,
        NAME,
        event_type,
        _unique_key,
        start_block,
        _inserted_timestamp
    FROM
        event_types qualify(ROW_NUMBER() over(PARTITION BY contract_address, _unique_key
    ORDER BY
        priority ASC, start_block DESC)) = 1
),
complete_abis AS (
    SELECT
        contract_address,
        proxy_address,
        abi_source,
        bytecode,
        COALESCE(
            proxy_address,
            contract_address
        ) AS abi_address,
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
        abi_priority
),
FINAL AS (
    SELECT
        COALESCE(
            b.contract_address,
            C.contract_address
        ) AS contract_address,
        C.abi_source,
        C.bytecode,
        C.proxy_address,
        C.complete_abi
    FROM
        complete_abis C
        LEFT JOIN {{ ref('silver__proxies') }}
        b
        ON C.abi_address = b.proxy_address
)
SELECT
    contract_address,
    ARRAY_AGG(
        DISTINCT abi_source
    ) AS abi_source,
    ARRAY_AGG(
        DISTINCT bytecode
    ) AS bytecode,
    ARRAY_AGG(
        DISTINCT proxy_address
    ) AS proxy_addresses,
    ARRAY_AGG(
        complete_abi
    ) AS abi
FROM
    FINAL
GROUP BY
    1
