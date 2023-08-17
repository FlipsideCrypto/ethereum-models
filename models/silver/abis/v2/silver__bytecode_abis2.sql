{{ config (
    materialized = "incremental",
    unique_key = "contract_address",
    tags = ['abis']
) }}

WITH contracts_with_abis AS (
    -- Identifying contracts with verified ABIs

    SELECT
        created_contract_address AS contract_address
    FROM
        {{ ref('silver__created_contracts') }}
        JOIN {{ ref('silver__verified_abis') }} A
        ON A.contract_address = created_contract_address
),
contracts_without_abis AS (
    -- Contracts that are missing ABIs
    SELECT
        created_contract_address AS contract_address,
        created_contract_input AS bytecode
    FROM
        {{ ref('silver__created_contracts') }}
    WHERE
        created_contract_address NOT IN (
            SELECT
                contract_address
            FROM
                contracts_with_abis
        )

{% if is_incremental() %}
AND created_contract_address NOT IN (
    SELECT
        contract_address
    FROM
        {{ this }}
)
{% endif %}
),
unique_bytecode_abis AS (
    -- Bytecodes from created_contracts with a unique ABI
    SELECT
        cc.created_contract_input AS bytecode,
        va.data AS abi,
        va.abi_hash
    FROM
        {{ ref('silver__created_contracts') }}
        cc
        JOIN {{ ref('silver__verified_abis') }}
        va
        ON cc.created_contract_address = va.contract_address
    GROUP BY
        cc.created_contract_input,
        va.data,
        va.abi_hash
    HAVING
        COUNT(
            DISTINCT va.data
        ) = 1 -- Ensuring there's only one ABI per bytecode
) -- Final matching
SELECT
    contract_address,
    abi,
    abi_hash,

{% if is_incremental() %}
SYSDATE()
{% else %}
    TO_TIMESTAMP_NTZ('2000-01-01 00:00:00')
{% endif %}

AS _inserted_timestamp
FROM
    contracts_without_abis
    JOIN unique_bytecode_abis USING (bytecode)
