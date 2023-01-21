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
        ROW_NUMBER() over(
            PARTITION BY bytecode
            ORDER BY
                abi_length DESC
        ) AS abi_row_no
    FROM
        (
            SELECT
                DISTINCT bytecode,
                abi,
                abi_hash,
                LENGTH(abi) AS abi_length
            FROM
                bytecodes
            WHERE
                abi_hash IS NOT NULL
        )
),
dupe_check AS (
    SELECT
        bytecode,
        COUNT(*) AS num_abis
    FROM
        bytecode_abis
    GROUP BY
        bytecode
    HAVING
        COUNT(*) > 1
)
SELECT
    contract_address,
    abi,
    abi_hash,
    SYSDATE() AS _inserted_timestamp,
    abi_row_no,
    CONCAT(
        contract_address,
        '-',
        abi_row_no
    ) AS abi_id,
    CASE
        WHEN num_abis > 1 THEN TRUE
        ELSE FALSE
    END AS bytecode_dupe
FROM
    contracts_missing_abis
    JOIN bytecode_abis USING (bytecode)
    LEFT JOIN dupe_check USING (bytecode)
