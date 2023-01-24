{{ config (
    materialized = "incremental",
    unique_key = "contract_address",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(contract_address)"
) }}

WITH override_abis AS (

    SELECT
        contract_address,
        PARSE_JSON(DATA) AS abi,
        TO_TIMESTAMP_LTZ(SYSDATE()) AS _inserted_timestamp,
        'flipside' AS abi_source,
        'flipside' AS discord_username,
        SHA2(abi) AS abi_hash,
        1 AS priority
    FROM
        {{ ref('silver__override_abis') }}
),
verified_abis AS (
    SELECT
        contract_address,
        DATA,
        _inserted_timestamp,
        abi_source,
        discord_username,
        abi_hash,
        2 AS priority
    FROM
        {{ ref('silver__verified_abis') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
        WHERE
            abi_source <> 'bytecode_matched'
    )
{% endif %}
),
bytecode_abis AS (
    SELECT
        contract_address,
        abi,
        abi_hash,
        'bytecode_matched' AS abi_source,
        NULL AS discord_username,
        _inserted_timestamp,
        3 AS priority
    FROM
        {{ ref('silver__bytecode_abis') }}
    WHERE
        NOT bytecode_dupe

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
    WHERE
        abi_source = 'bytecode_matched'
)
{% endif %}
),
all_abis AS (
    SELECT
        contract_address,
        abi AS DATA,
        _inserted_timestamp,
        abi_source,
        discord_username,
        abi_hash,
        priority
    FROM
        override_abis
    UNION
    SELECT
        contract_address,
        DATA,
        _inserted_timestamp,
        abi_source,
        discord_username,
        abi_hash,
        priority
    FROM
        verified_abis
    UNION
    SELECT
        contract_address,
        abi AS DATA,
        _inserted_timestamp,
        abi_source,
        discord_username,
        abi_hash,
        priority
    FROM
        bytecode_abis
),
priority_abis AS (
    SELECT
        contract_address,
        DATA,
        _inserted_timestamp,
        abi_source,
        discord_username,
        abi_hash,
        priority
    FROM
        all_abis qualify(ROW_NUMBER() over(PARTITION BY contract_address
    ORDER BY
        priority ASC)) = 1
)
SELECT
    p.contract_address,
    p.data,
    p._inserted_timestamp,
    p.abi_source,
    p.discord_username,
    p.abi_hash,
    created_contract_input AS bytecode
FROM
    priority_abis p
    LEFT JOIN {{ ref('silver__created_contracts') }}
    ON p.contract_address = created_contract_address
