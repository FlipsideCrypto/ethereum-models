{{ config (
    materialized = "incremental",
    unique_key = "abi_id"
) }}

WITH bytecodes AS (

    SELECT
        created_contract_address AS contract_address,
        A.data AS abi,
        created_contract_input AS bytecode,
        abi_hash
    FROM
        {{ ref('silver__created_contracts') }}
        LEFT JOIN {{ ref('silver__verified_abis') }} A
        ON A.contract_address = created_contract_address

{% if is_incremental() %}
AND created_contract_address NOT IN (
    SELECT
        contract_address
    FROM
        {{ this }}
)
{% endif %}
),
contracts_missing_abis AS (
    SELECT
        contract_address,
        bytecode
    FROM
        bytecodes
    WHERE
        abi_hash IS NULL
),
bytecode_abis AS (
    SELECT
        *,
        ROW_NUMBER() over(PARTITION BY bytecode
    ORDER BY
        abi_hash DESC) as ABI_ROW_NO
    FROM
        (
            SELECT
                DISTINCT bytecode,
                abi,
                abi_hash
            FROM
                bytecodes
            WHERE
                abi_hash IS NOT NULL
        ) 
)
SELECT
    contract_address,
    abi,
    abi_hash,
    SYSDATE() AS _inserted_timestamp,
    ABI_ROW_NO,
    concat(contract_address, '-', ABI_ROW_NO) AS abi_id
FROM
    contracts_missing_abis
    JOIN bytecode_abis USING (bytecode)
