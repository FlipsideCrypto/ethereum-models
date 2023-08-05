{{ config (
    materialized = "view"
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 3
)
SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['created_contract_address', 'block_number']
    ) }} AS id,
    created_contract_address AS contract_address,
    block_number
FROM
    {{ ref("silver__created_contracts") }}
WHERE
    block_number < (
        SELECT
            block_number
        FROM
            last_3_days
    )
EXCEPT
SELECT
    id,
    contract_address,
    block_number
FROM
    {{ ref("streamline__complete_contract_abis") }}
WHERE
    block_number < (
        SELECT
            block_number
        FROM
            last_3_days
    )
ORDER BY
    block_number
