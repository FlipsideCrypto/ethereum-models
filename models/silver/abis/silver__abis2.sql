{{ config (
    materialized = "incremental",
    unique_key = "contract_address",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(contract_address)"
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
        MAX(start_block) AS start_block,
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
        6
),
abi_priority AS (
    SELECT
        contract_address,
        proxy_address,
        priority,
        inputs,
        anonymous,
        NAME,
        event_type,
        _unique_key,
        start_block,
        _inserted_timestamp
    FROM
        event_types qualify(ROW_NUMBER() over(PARTITION BY _unique_key
    ORDER BY
        priority ASC, start_block DESC)) = 1
),
FINAL AS (
    SELECT
        contract_address,
        proxy_address,
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
)
SELECT
    b.contract_address,
    b.abi_source,
    b.bytecode,
    ARRAY_AGG(
        f.complete_abi
    ) AS abi
FROM
    FINAL f
    LEFT JOIN abi_base b
    ON b.proxy_address = f.abi_address
GROUP BY
    1,
    2,
    3
