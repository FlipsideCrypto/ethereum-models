{{ config (
    materialized = 'table'
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
            MAX(_inserted_timestamp) :: DATE
        FROM
            {{ this }}
    )
{% endif %}
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
        VALUE :name :: STRING AS NAME,
        _inserted_timestamp
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
        _inserted_timestamp,
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
        NAME,
        _inserted_timestamp
),
proxy_base AS (
    SELECT
        C.created_contract_address AS contract_address,
        p.proxy_address,
        p.start_block,
        C.block_number AS created_block
    FROM
        {{ ref('silver__created_contracts') }} C
        INNER JOIN {{ ref('silver__proxies2') }}
        p
        ON C.created_contract_address = p.contract_address
),
FINAL AS (
    SELECT
        ea.contract_address,
        ea.inputs,
        ea.anonymous,
        ea.name,
        ea.event_type,
        pb.start_block,
        pb.contract_address AS base_contract_address
    FROM
        event_types ea
        INNER JOIN proxy_base pb
        ON ea.contract_address = pb.proxy_address
    UNION
    SELECT
        eab.contract_address,
        eab.inputs,
        eab.anonymous,
        eab.name,
        eab.event_type,
        pbb.created_block AS start_block,
        pbb.contract_address AS base_contract_address
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
    UNION
    SELECT
        eac.contract_address,
        eac.inputs,
        eac.anonymous,
        eac.name,
        eac.event_type,
        0 AS start_block,
        eac.contract_address AS base_contract_address
    FROM
        event_types eac
    WHERE
        contract_address NOT IN (
            SELECT
                DISTINCT contract_address
            FROM
                proxy_base
        )
)
SELECT
    contract_address AS source_contract_address,
    base_contract_address AS parent_contract_address,
    NAME AS event_name,
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
    PARSE_JSON(
        complete_abi :: STRING
    ) AS abi,
    start_block,
    IFNULL(LEAD(start_block) over (PARTITION BY base_contract_address, NAME, event_type
ORDER BY
    start_block) -1, 1e18) AS end_block,
    ethereum_dev.silver.udf_encode_atb(
        CONCAT(
            NAME,
            '(',
            REPLACE(REPLACE(REPLACE(event_type :: STRING, '"'), '['), ']'),
            ')'
        )
    ) AS event_sig
FROM
    FINAL
